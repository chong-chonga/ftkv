package shardkv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ftkv/v1/raft"
	"github.com/ftkv/v1/router"
	"github.com/ftkv/v1/shardkv/shardproto"
	"github.com/ftkv/v1/tool"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	keyNotExistsCommandTerm = -1

	wrongGroupTerm = -2

	configMismatchTerm = -3
)

type server struct {
	shardproto.ShardKVServer

	channelMu    *sync.Mutex
	transferCond *sync.Cond
	shardMu      *sync.Mutex

	//password       string
	//sessionTimeout time.Duration
	//sessionMap     map[string]time.Time

	logEnabled       bool
	maxRaftStateSize int
	rf               *Raft
	applyCh          chan applyMsg
	storage          *tool.Storage
	waitingClients   map[int]chan commitResult

	shardkvId       string
	groupId         int32
	routerAddresses []string

	// persistent
	snapshotIndex int
	uniqueId      int64
	ht            map[string]string

	configId         int32
	inConfiguration  bool
	shards           map[int]byte
	shardsToGet      map[int]byte
	shardsToTransfer map[int32][]int
	replicaGroups    map[int32][]string
	nextConfig       int
}

type ServerConfigWrapper struct {
	ServerConfig ServerConfig `yaml:"shardkvServer"`
}

type ServerConfig struct {
	ShardKVId        string      `yaml:"shardkvId"`
	GroupId          int32       `yaml:"groupId"`
	RouterAddresses  []string    `yaml:"routerAddresses"`
	Port             int         `yaml:"port"`
	MaxRaftStateSize int         `yaml:"maxRaftStateSize"`
	LogEnabled       bool        `yaml:"logEnabled"`
	Raft             raft.Config `yaml:"raft"`
}

func readServerConfig(config []byte) (*ServerConfig, error) {
	if nil == config || len(config) == 0 {
		return nil, errors.New("configuration is empty")
	}
	conf := &ServerConfigWrapper{}
	err := yaml.Unmarshal(config, conf)
	if err != nil {
		return nil, err
	}
	serverConfig := &conf.ServerConfig
	if serverConfig.Port == 0 {
		return nil, errors.New("port is not configured")
	}
	if len(serverConfig.ShardKVId) == 0 {
		return nil, errors.New("shardkvId is not configured")
	}
	return serverConfig, nil
}

type StopFunc func()

//
// StartShardKVServer starts a shardkv server which is responsible for a subset of the shards.
// A replica consists of a handful of servers that use Raft to replicate the group's shards
//
func StartShardKVServer(configData []byte) (string, StopFunc, error) {
	config, err := readServerConfig(configData)
	if err != nil {
		return "", nil, &tool.RuntimeError{Stage: "load config", Err: err}
	}

	port := config.Port
	if port <= 0 {
		return "", nil, &tool.RuntimeError{Stage: "configure shardkv server", Err: errors.New("server port " + strconv.Itoa(port) + " is invalid")}
	}
	routerClient, err := router.MakeServiceClient(config.RouterAddresses)
	if err != nil {
		return "", nil, &tool.RuntimeError{Stage: "configure shardkv server", Err: err}
	}

	//timeout := config.SessionTimeout
	//if timeout <= 0 {
	//	return "", nil, &tool.RuntimeError{Stage: "configure shardkv server", Err: errors.New("session timeout must be positive")}
	//}

	routerId := config.ShardKVId
	serviceName := "shardkv-" + routerId
	storage, err := tool.MakeStorage(serviceName)
	if err != nil {
		return "", nil, &tool.RuntimeError{Stage: "make storage", Err: err}
	}

	shardkv := new(server)

	snapshot := storage.GetSnapshot()
	if nil != snapshot && len(snapshot) > 0 {
		if err = shardkv.restore(snapshot); err != nil {
			return "", nil, &tool.RuntimeError{Stage: "restore", Err: err}
		}
	} else {
		shardkv.initPersistentState()
	}
	shardkv.channelMu = &sync.Mutex{}
	shardkv.shardMu = &sync.Mutex{}
	shardkv.transferCond = sync.NewCond(shardkv.shardMu)

	shardkv.shardkvId = routerId
	shardkv.groupId = config.GroupId
	shardkv.routerAddresses = config.RouterAddresses
	//shardkv.password = config.Password
	//shardkv.sessionTimeout = time.Duration(timeout) * time.Second
	//shardkv.sessionMap = make(map[string]time.Time)
	//log.Printf("configure shardkv info: session expireTime=%ds")

	shardkv.storage = storage
	shardkv.waitingClients = make(map[int]chan commitResult)

	// configure router
	maxRaftStateSize := config.MaxRaftStateSize
	shardkv.maxRaftStateSize = maxRaftStateSize
	if maxRaftStateSize > 0 {
		log.Printf("configure shardkv info: service will make a snapshot when Raft state size bigger than %d bytes", maxRaftStateSize)
	} else {
		log.Println("configure shardkv info: disable snapshot")
	}

	if config.LogEnabled {
		shardkv.logEnabled = true
		log.Println("configure shardkv info: enable service log")
	} else {
		log.Println("configure shardkv info: disable service log")
	}

	// start listener
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		err = &tool.RuntimeError{Stage: "start listener", Err: err}
		return "", nil, err
	}

	applyCh := make(chan applyMsg)
	shardkv.applyCh = applyCh
	// initialize success, start Raft
	raftConfig := config.Raft
	shardkv.rf, err = startRaft(raftConfig, storage, applyCh)
	if err != nil {
		_ = listener.Close()
		return "", nil, err
	}

	go shardkv.listenConfig(routerClient)
	go shardkv.transferShards()
	go shardkv.executeCommands()

	// start grpc server
	s := grpc.NewServer()
	shardproto.RegisterShardKVServer(s, shardkv)
	go func() {
		_ = s.Serve(listener)
	}()

	log.Printf("start shardkv server success, serviceName=%s, serves at port:%d", serviceName, port)
	return serviceName, s.Stop, nil
}

func (s *server) initPersistentState() {
	s.snapshotIndex = 0
	s.uniqueId = 0
	s.ht = make(map[string]string)

	s.configId = 0
	s.inConfiguration = false
	s.shards = make(map[int]byte)
	s.shardsToGet = make(map[int]byte)
	s.shardsToTransfer = make(map[int32][]int)
	s.replicaGroups = make(map[int32][]string)
}

func (s *server) logPrintf(method string, format string, a ...interface{}) {
	if s.logEnabled {
		str := fmt.Sprintf("["+(s.shardkvId)+"] "+method+": "+format, a...)
		log.Println(str)
	}
}

func (s *server) makeSnapshot() (error, []byte) {
	w := new(bytes.Buffer)
	e := json.NewEncoder(w)
	var err error
	if err = e.Encode(s.snapshotIndex); err != nil {
		return errors.New("encode snapshotIndex fails: " + err.Error()), nil
	}
	if err = e.Encode(s.uniqueId); err != nil {
		return errors.New("encode uniqueId fails: " + err.Error()), nil
	}
	if err = e.Encode(s.ht); err != nil {
		return errors.New("encode ht fails: " + err.Error()), nil
	}

	if err = e.Encode(s.configId); err != nil {
		return errors.New("encode configId fails: " + err.Error()), nil
	}
	if err = e.Encode(s.inConfiguration); err != nil {
		return errors.New("encode inConfiguration fails: " + err.Error()), nil
	}
	if err = e.Encode(s.shards); err != nil {
		return errors.New("encode shards fails: " + err.Error()), nil
	}
	if err = e.Encode(s.shardsToGet); err != nil {
		return errors.New("encode shardsToGet fails: " + err.Error()), nil
	}
	if err = e.Encode(s.shardsToTransfer); err != nil {
		return errors.New("encode shardsToTransfer fails: " + err.Error()), nil
	}
	if err = e.Encode(s.replicaGroups); err != nil {
		return errors.New("encode replicaGroups fails: " + err.Error()), nil
	}
	s.logPrintf("makeSnapshot:", "has made a snapshot, snapshotIndex=%d", s.snapshotIndex)
	return nil, w.Bytes()
}

func (s *server) restore(snapshot []byte) error {
	if len(snapshot) == 0 {
		return errors.New("snapshot is nil")
	}
	r := bytes.NewBuffer(snapshot)
	d := json.NewDecoder(r)

	var snapshotIndex int
	var uniqueId int64
	var ht map[string]string

	var configId int32
	var inConfiguration bool
	var shards map[int]byte
	var shardsToGet map[int]byte
	var shardsToTransfer map[int32][]int
	var replicaGroups map[int32][]string
	var err error
	if err = d.Decode(&snapshotIndex); err != nil {
		return errors.New("decode snapshotIndex fails: " + err.Error())
	}
	if err = d.Decode(&uniqueId); err != nil {
		return errors.New("decode uniqueId fails: " + err.Error())
	}
	if err = d.Decode(&ht); err != nil {
		return errors.New("decode ht fails: " + err.Error())
	}

	if err = d.Decode(&configId); err != nil {
		return errors.New("decode configId fails: " + err.Error())
	}
	if err = d.Decode(&inConfiguration); err != nil {
		return errors.New("decode inConfiguration fails: " + err.Error())
	}
	if err = d.Decode(&shards); err != nil {
		return errors.New("decode shards fails: " + err.Error())
	}
	if err = d.Decode(&shardsToGet); err != nil {
		return errors.New("decode shardsToGet fails: " + err.Error())
	}
	if err = d.Decode(&shardsToTransfer); err != nil {
		return errors.New("decode shardsToTransfer fails: " + err.Error())
	}
	if err = d.Decode(&replicaGroups); err != nil {
		return errors.New("decode replicaGroups fails: " + err.Error())
	}
	s.snapshotIndex = snapshotIndex
	s.uniqueId = uniqueId
	s.ht = ht

	s.configId = configId
	s.inConfiguration = inConfiguration
	s.shards = shards
	s.shardsToGet = shardsToGet
	s.shardsToTransfer = shardsToTransfer
	s.replicaGroups = replicaGroups
	s.logPrintf("restore", "restore snapshot success, snapshotIndex=%d", snapshotIndex)
	return nil
}

type commitResult struct {
	term   int
	result string
}

func (s *server) Get(_ context.Context, args *shardproto.GetRequest) (*shardproto.GetReply, error) {
	c := command{
		CommandType: commandType_Get,
		Key:         args.Key,
		Shard:       router.Key2Shard(args.Key),
	}
	reply := &shardproto.GetReply{}
	reply.ErrCode, reply.Value = s.submit(c)
	if reply.ErrCode == shardproto.ResponseCode_OK {
		reply.Exist = true
	}
	reply.ErrMessage = reply.ErrCode.String()
	return reply, nil
}

func (s *server) Set(_ context.Context, args *shardproto.SetRequest) (*shardproto.SetReply, error) {
	c := command{
		CommandType: commandType_Set,
		Key:         args.Key,
		Value:       args.Value,
		Shard:       router.Key2Shard(args.Key),
	}
	reply := &shardproto.SetReply{}
	reply.ErrCode, _ = s.submit(c)
	reply.ErrMessage = reply.ErrCode.String()
	return reply, nil
}

func (s *server) Delete(_ context.Context, args *shardproto.DeleteRequest) (*shardproto.DeleteReply, error) {
	c := command{
		CommandType: commandType_Delete,
		Key:         args.Key,
		Shard:       router.Key2Shard(args.Key),
	}
	reply := &shardproto.DeleteReply{}
	reply.ErrCode, _ = s.submit(c)
	reply.ErrMessage = reply.ErrCode.String()
	return reply, nil
}

func (s *server) Transfer(_ context.Context, args *shardproto.TransferRequest) (*shardproto.TransferReply, error) {
	c := command{
		CommandType: commandType_Shards_Transfer,
		ConfigId:    args.ConfigId,
		Shards:      args.Shards,
		Data:        args.Data,
	}
	reply := &shardproto.TransferReply{}
	reply.ErrCode, _ = s.submit(c)
	reply.ErrMessage = reply.ErrCode.String()
	return reply, nil
}

func (s *server) configure(ownShards map[int]byte, groupId int32, configId int32, shardsMap []int32, replicaGroups map[int32][]string) {
	shardsToTransfer := make(map[int32][]int)
	shardsToGet := make(map[int]byte)
	for shard, ownGid := range shardsMap {
		if b, exist := ownShards[shard]; exist && b == 1 {
			if ownGid != groupId {
				shards := shardsToTransfer[ownGid]
				shards = append(shards, shard)
				shardsToTransfer[ownGid] = shards
			}
		} else {
			if ownGid == groupId {
				shardsToGet[shard] = 1
			}
		}

	}
	c := command{
		CommandType:      commandType_Configure,
		ConfigId:         configId,
		ShardsToGet:      shardsToGet,
		ShardsToTransfer: shardsToTransfer,
		ReplicaGroups:    replicaGroups,
	}
	s.submit(c)
}

func (s *server) deleteShards(configId int32, groupId int32, keys []string) bool {
	c := command{
		CommandType: commandType_Shards_Delete,
		ConfigId:    configId,
		GroupId:     groupId,
		Keys:        keys,
	}
	errCode, _ := s.submit(c)
	return errCode == shardproto.ResponseCode_OK
}

func (s *server) submit(command command) (shardproto.ResponseCode, string) {
	commandIndex, commandTerm, isLeader := s.rf.Start(command)
	if !isLeader {
		s.logPrintf("submit", "not leader, refuse command:%s", command.CommandString())
		return shardproto.ResponseCode_WRONG_LEADER, ""
	}
	// leader1(current leader) may be partitioned by itself,
	// its log may be trimmed by leader2 (if and only if leader2's term > leader1's term)
	// but len(leader2's log) may less than len(leader1's log)
	// if leader1 becomes leader again, then commands submitted later may get the same log index
	// that's to say, previously submitted commands will never be completed

	// channel in go is implemented through wait list and ring buffer
	ch := make(chan commitResult, 1)
	s.channelMu.Lock()
	if c, exist := s.waitingClients[commandIndex]; exist && c != nil {
		// tell the previous client to stop waiting
		c <- commitResult{term: commandTerm}
		close(c)
	}
	s.waitingClients[commandIndex] = ch
	s.channelMu.Unlock()
	s.logPrintf("submit", "waiting command to finish, commandIndex=%d, commandTerm=%d, command:%s", commandIndex, commandTerm, command.CommandString())

	res := <-ch

	receivedTerm := res.term
	s.logPrintf("submit", "expected commandTerm=%d for commandIndex=%d, received commandTerm=%d, config=%v", commandTerm, commandIndex, receivedTerm, res.result)
	// log's index and term identifies the unique log

	if receivedTerm == commandTerm {
		return shardproto.ResponseCode_OK, res.result
	}

	switch receivedTerm {
	case keyNotExistsCommandTerm:
		return shardproto.ResponseCode_KEY_NOT_EXISTS, ""
	case wrongGroupTerm:
		return shardproto.ResponseCode_WRONG_GROUP, ""
	case configMismatchTerm:
		return shardproto.ResponseCode_CONFIG_MISMATCH, ""
	default:
		return shardproto.ResponseCode_WRONG_LEADER, ""
	}
}

func (s *server) listenConfig(routerClient *router.ServiceClient) {
	shardMu := s.shardMu
	groupId := s.groupId
	for {
		if s.isLeader() {
			shardMu.Lock()
			configurable := !s.inConfiguration
			nextConfigId := s.configId + 1
			shardMu.Unlock()
			if configurable {
				config, err := routerClient.Query(nextConfigId)
				if err != nil {
					s.logPrintf("listenConfig", "query fail: %v", err)
				} else {
					s.logPrintf("listenConfig", "get config: %v", config)
					if config.ConfigId == nextConfigId {
						shardMu.Lock()
						if s.configId+1 == nextConfigId {
							shards := copyShards(s.shards)
							shardMu.Unlock()
							replicaGroups := make(map[int32][]string)
							for gid, servers := range config.ReplicaGroups {
								replicaGroups[gid] = servers.Servers
							}
							s.configure(shards, groupId, config.ConfigId, config.ShardsMap, replicaGroups)
						} else {
							shardMu.Unlock()
						}
					}
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func copyShards(src map[int]byte) map[int]byte {
	dst := make(map[int]byte)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func (s *server) isLeader() bool {
	_, isLeader := s.rf.GetState()
	return isLeader
}

func (s *server) transferShards() {
	shardMu := s.shardMu
	cond := s.transferCond
	lastTransferConfig := 0
	for {
		if !s.isLeader() {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		shardMu.Lock()
		for len(s.shardsToTransfer) == 0 || lastTransferConfig == int(s.configId) {
			cond.Wait()
		}
		configId := s.configId
		shardsToMove := copyShardsToTransfer(s.shardsToTransfer)
		finished := true
		for gid, shards := range shardsToMove {
			if configId != s.configId {
				finished = false
				break
			}
			if !hasTransferred(gid, s.shardsToTransfer) {
				data := make(map[string]string)
				var keys []string
				for k, v := range s.ht {
					shard := router.Key2Shard(k)
					for _, e := range shards {
						if shard == e {
							data[k] = v
							keys = append(keys, k)
							break
						}
					}
				}
				addrs := s.replicaGroups[gid]
				shardMu.Unlock()
				if len(addrs) > 0 {
					client := MakeServiceClient(addrs)
					shardMu.Lock()
					if !s.isLeader() || s.configId != configId {
						break
					}
					shardMu.Unlock()
					int32Shards := make([]int32, len(shards))
					for i, shard := range shards {
						int32Shards[i] = int32(shard)
					}
					err := client.Transfer(configId, int32Shards, data)
					for err != nil {
						time.Sleep(200 * time.Millisecond)
						err = client.Transfer(configId, int32Shards, data)
					}
				}
				finished = s.deleteShards(configId, gid, keys)
				shardMu.Lock()
				if !finished {
					break
				}
			}
		}
		if finished {
			lastTransferConfig = int(configId)
			s.logPrintf("dataTransfer", "finish current move, switch to next")
		}

		shardMu.Unlock()
	}
}

func hasTransferred(gid int32, shardsToTransfer map[int32][]int) bool {
	if shards, needTransfer := shardsToTransfer[gid]; needTransfer && shards != nil {
		return false
	}
	return true
}

func copyShardsToTransfer(src map[int32][]int) map[int32][]int {
	dst := make(map[int32][]int)
	for gid, shards := range src {
		dst[gid] = shards
	}
	return dst
}

func (s *server) executeCommands() {
	maxRaftStateSize := s.maxRaftStateSize
	snapshotIndex := s.snapshotIndex
	expectedIndex := snapshotIndex + 1
	enableSnapshot := maxRaftStateSize > 0
	for {
		msg := <-s.applyCh
		if msg.CommandValid {
			commandIndex := msg.CommandIndex
			if commandIndex != expectedIndex {
				log.Fatalf("[%s] expected log inedx is %d, but received is %d, applyMsg=%v", s.shardkvId, expectedIndex, commandIndex, msg)
			}
			expectedIndex++
			c := msg.Command
			s.logPrintf("executeCommands", "receive committed command:%s", c.CommandString())
			ct := c.CommandType
			value := ""
			commandTerm := msg.CommandTerm
			switch ct {
			case commandType_Get, commandType_Set, commandType_Delete:
				value, commandTerm = s.handleClientRequest(c, commandTerm)
				break
			case commandType_Configure:
				s.handleConfig(c)
				break
			case commandType_Shards_Transfer:
				commandTerm = s.handleShardsTransfer(c, commandTerm)
				break
			case commandType_Shards_Delete:
				s.handleShardsDelete(c)
				break
			default:
				log.Fatalf("[%s] receive unknown type operation, command:%s", s.shardkvId, c.CommandString())
			}
			s.replyClientIfNeeded(commandIndex, commandTerm, value)
			if enableSnapshot && s.storage.RaftStateSize() >= maxRaftStateSize {
				s.snapshotIndex = commandIndex
				err, snapshot := s.makeSnapshot()
				if err != nil {
					log.Fatalln("make snapshot fail: " + err.Error())
				}
				s.rf.Snapshot(commandIndex, snapshot)
			}

		} else if msg.SnapshotValid {
			err := s.restore(msg.Snapshot)
			if err != nil {
				log.Fatalln(err)
			}
			snapshotIndex = s.snapshotIndex
			if snapshotIndex != msg.SnapshotIndex {
				log.Fatalf("[%s] service snapshot index is %d, but Raft snapshot index is %d", s.shardkvId,
					snapshotIndex, msg.SnapshotIndex)
			}
			if snapshotIndex < expectedIndex {
				log.Fatalf("[%s] received snapshot is out-dated, expected log index=%d, received snapshot index=%d", s.shardkvId, expectedIndex, snapshotIndex)
			}
			expectedIndex = snapshotIndex + 1
		} else {
			log.Fatalf("[%s] receive unknown type meesage from Raft, applyMsg=%v", s.shardkvId, msg)
		}
	}
}

func (s *server) replyClientIfNeeded(commandIndex int, commandTerm int, result string) {
	s.channelMu.Lock()
	if ch, _ := s.waitingClients[commandIndex]; ch != nil {
		s.logPrintf("executeCommands", "send commandTerm=%d for commandIndex=%d, value=%v", commandTerm, commandIndex, result)
		ch <- commitResult{
			term:   commandTerm,
			result: result,
		}
		close(ch)
		delete(s.waitingClients, commandIndex)
	}
	s.channelMu.Unlock()
}

func (s *server) handleClientRequest(c command, commandTerm int) (string, int) {
	shardMu := s.shardMu
	shard := c.Shard
	value := ""
	shardMu.Lock()
	if b, exist := s.shards[shard]; exist && b == 1 {
		if c.CommandType == commandType_Set {
			s.ht[c.Key] = c.Value
			s.logPrintf("handleClientRequest", "set %s -> %s", c.Key, c.Value)
		} else if c.CommandType == commandType_Delete {
			delete(s.ht, c.Key)
			s.logPrintf("handleClientRequest", "delete key %s", c.Key)
		} else {
			value, exist = s.ht[c.Key]
			if !exist {
				commandTerm = keyNotExistsCommandTerm
			}
			s.logPrintf("handleClientRequest", "query key %s, value=%s, exist=%v", c.Key, value, exist)
		}
	} else {
		commandTerm = wrongGroupTerm
		s.logPrintf("handleClientRequest", "can not handle request for shard:%d, current shards:%v", shard, s.shards)
	}
	shardMu.Unlock()
	return value, commandTerm
}

func (s *server) shardInfo() string {
	shardsToTransfer := make([]int, 0)
	for _, shards := range s.shardsToTransfer {
		shardsToTransfer = append(shardsToTransfer, shards...)
	}
	shardsToGet := make([]int, len(s.shardsToGet))
	i := 0
	for shard := range s.shardsToGet {
		shardsToGet[i] = shard
		i++
	}
	currentShards := make([]int, len(s.shards))
	i = 0
	for shard := range s.shards {
		currentShards[i] = shard
		i++
	}
	return fmt.Sprintf("shards:%v need to transfer, shards:%v need to get, "+"responsible for shards:%v currently", shardsToTransfer, shardsToGet, currentShards)
}

func (s *server) handleConfig(c command) {
	nextConfigId := s.configId + 1
	configId := c.ConfigId
	if configId > nextConfigId {
		log.Fatalf("[%s] expected configuration is config:%d, but receive config:%d "+s.shardInfo(), s.shardkvId, nextConfigId, c.ConfigId)
	} else if configId == nextConfigId {
		shardMu := s.shardMu
		shardMu.Lock()

		if !s.inConfiguration {
			s.logPrintf("handleConfig", "receive config:%d", configId)
			shards := s.shards
			// initial configuration?
			if configId == 1 {
				for shard := range c.ShardsToGet {
					shards[shard] = 1
				}
				s.logPrintf("handleConfig", "use initial config:%d, responsible for shards:%v currently", configId, c.ShardsToGet)
			} else {
				if len(c.ShardsToGet) != 0 || len(c.ShardsToTransfer) != 0 {
					s.inConfiguration = true
					for _, shardsToMove := range c.ShardsToTransfer {
						for _, shard := range shardsToMove {
							delete(shards, shard)
						}
					}
					s.shardsToTransfer = c.ShardsToTransfer
					s.shardsToGet = c.ShardsToGet
					s.replicaGroups = c.ReplicaGroups
					// transfer shards if needed
					if len(c.ShardsToTransfer) != 0 {
						s.transferCond.Signal()
					}
				}
				s.logPrintf("handleConfig", "use config:%d, "+s.shardInfo(), configId)
			}
			s.configId = configId
		}
		shardMu.Unlock()
	} // else... config is out-dated, ignore it
}

func (s *server) handleShardsTransfer(c command, commandTerm int) int {
	if c.ConfigId > s.configId {
		commandTerm = configMismatchTerm
	} else if c.ConfigId == s.configId {
		shardMu := s.shardMu
		shardMu.Lock()
		duplicate := false
		shards := s.shards
		for _, shard := range c.Shards {
			if b, exist := shards[int(shard)]; exist && b == 1 {
				duplicate = true
				break
			}
		}
		if !duplicate {
			shardsToGet := s.shardsToGet
			for _, shard := range c.Shards {
				shards[int(shard)] = 1
				delete(shardsToGet, int(shard))
			}
			for k, v := range c.Data {
				s.ht[k] = v
			}

			s.logPrintf("handleShardsTransfer", "received data in shards:%v, "+s.shardInfo()+", config:%d", c.Shards, c.ConfigId)
			if len(s.shardsToTransfer) == 0 && len(s.shardsToGet) == 0 {
				s.inConfiguration = false
			}
		}
		shardMu.Unlock()
	} // else... config is out-dated, ignore it
	return commandTerm
}

func (s *server) handleShardsDelete(c command) {
	if c.ConfigId > s.configId {
		log.Fatalf("[%d] can not remove shards in subsequent configuation, deleteArgs:%v configId:%d "+s.shardInfo(), s.groupId, c, c.ConfigId)
	} else if c.ConfigId == s.configId {
		if movedShards, exist := s.shardsToTransfer[c.GroupId]; exist {
			shardMu := s.shardMu
			shardMu.Lock()
			delete(s.shardsToTransfer, c.GroupId)
			for _, key := range c.Keys {
				delete(s.ht, key)
			}
			s.logPrintf("handleShardsDelete", "has transferred shards:%v, "+s.shardInfo()+", config:%d", movedShards, s.configId)
			if len(s.shardsToTransfer) == 0 && len(s.shardsToGet) == 0 {
				s.inConfiguration = false
			}
			shardMu.Unlock()
		}
	} // else... config is out-dated, ignore it
}
