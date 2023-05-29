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
	"github.com/ftkv/v1/storage"
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
	storage          *storage.Storage
	waitingClients   map[int]chan int

	shardkvId       string
	groupId         int32
	routerAddresses []string

	// persistent
	snapshotIndex int
	uniqueId      int64
	ht            map[string]string

	currentConfigId     int32
	noGroupsAliveBefore bool
	reshard             bool
	shards              map[int]byte
	shardsToGet         map[int]byte
	shardsToTransfer    map[int32][]int
	replicaGroups       map[int32][]string
	nextConfig          int
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
	routerClient, err := router.MakeListenerClient(config.RouterAddresses)
	if err != nil {
		return "", nil, &tool.RuntimeError{Stage: "configure shardkv server", Err: err}
	}

	//timeout := config.SessionTimeout
	//if timeout <= 0 {
	//	return "", nil, &tool.RuntimeError{Stage: "configure shardkv server", Err: errors.New("session timeout must be positive")}
	//}

	shardKVId := config.ShardKVId
	groupId := config.GroupId
	serviceName := "shardkv-" + strconv.Itoa(int(groupId)) + "-" + shardKVId
	storage, err := storage.MakeStorage(serviceName)
	if err != nil {
		return "", nil, &tool.RuntimeError{Stage: "make storage", Err: err}
	}

	shardkv := new(server)
	shardkv.channelMu = &sync.Mutex{}
	shardkv.shardMu = &sync.Mutex{}
	shardkv.transferCond = sync.NewCond(shardkv.shardMu)

	snapshot := storage.GetSnapshot()
	if nil != snapshot && len(snapshot) > 0 {
		if err = shardkv.restore(snapshot); err != nil {
			return "", nil, &tool.RuntimeError{Stage: "restore", Err: err}
		}
	} else {
		shardkv.initPersistentState()
	}

	shardkv.shardkvId = shardKVId
	shardkv.groupId = groupId
	shardkv.routerAddresses = config.RouterAddresses
	//shardkv.password = config.Password
	//shardkv.sessionTimeout = time.Duration(timeout) * time.Second
	//shardkv.sessionMap = make(map[string]time.Time)
	//log.Printf("configure shardkv info: session expireTime=%ds")

	shardkv.storage = storage
	shardkv.waitingClients = make(map[int]chan int)

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

	s.currentConfigId = 0
	s.noGroupsAliveBefore = true
	s.reshard = false
	s.shards = make(map[int]byte)
	s.shardsToGet = make(map[int]byte)
	s.shardsToTransfer = make(map[int32][]int)
	s.replicaGroups = make(map[int32][]string)
}

func (s *server) logPrintf(method string, format string, a ...interface{}) {
	if s.logEnabled {
		prefix := "[" + strconv.FormatInt(int64(s.groupId), 10) + "-" + s.shardkvId + "] " + method + ": "
		str := fmt.Sprintf(prefix+format, a...)
		log.Println(str)
	}
}

func (s *server) logFatalf(method string, format string, a ...interface{}) {
	prefix := "[" + strconv.FormatInt(int64(s.groupId), 10) + "-" + s.shardkvId + "] " + method + ": "
	str := fmt.Sprintf(prefix+format, a...)
	log.Fatalln(str)
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

	if err = e.Encode(s.currentConfigId); err != nil {
		return errors.New("encode currentConfigId fails: " + err.Error()), nil
	}
	if err = e.Encode(s.noGroupsAliveBefore); err != nil {
		return errors.New("encode noGroupsAliveBefore fails: " + err.Error()), nil
	}
	if err = e.Encode(s.reshard); err != nil {
		return errors.New("encode reshard fails: " + err.Error()), nil
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
	var noGroupsAliveBefore bool
	var reshard bool
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
		return errors.New("decode currentConfigId fails: " + err.Error())
	}
	if err = d.Decode(&noGroupsAliveBefore); err != nil {
		return errors.New("decode noGroupsAliveBefore fails: " + err.Error())
	}
	if err = d.Decode(&reshard); err != nil {
		return errors.New("decode reshard fails: " + err.Error())
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
	s.shardMu.Lock()
	s.snapshotIndex = snapshotIndex
	s.uniqueId = uniqueId
	s.ht = ht

	s.currentConfigId = configId
	s.noGroupsAliveBefore = noGroupsAliveBefore
	s.reshard = reshard
	s.shards = shards
	s.shardsToGet = shardsToGet
	s.shardsToTransfer = shardsToTransfer
	s.replicaGroups = replicaGroups
	s.shardMu.Unlock()
	s.logPrintf("restore", "restore snapshot success, snapshotIndex=%d", snapshotIndex)
	return nil
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

func (s *server) isLeader() bool {
	_, isLeader := s.rf.GetState()
	return isLeader
}

func (s *server) Get(_ context.Context, args *shardproto.GetRequest) (*shardproto.GetReply, error) {
	key := args.Key
	c := command{
		CommandType: commandType_Get,
	}
	reply := &shardproto.GetReply{}

	errcode := s.submit(c)
	value := ""
	exist := false

	fmt.Printf("errcode=%v, target=%v, %v", errcode, shardproto.ResponseCode_OK, errcode == shardproto.ResponseCode_OK)
	if errcode == shardproto.ResponseCode_OK {
		shard := router.Key2Shard(key)
		shardMu := s.shardMu
		// check if shard exists first, then fetch the data
		// 1. the group which receives the data, will save data first, then update shards
		// 2. then group which transfers data will delete shards first, then delete data
		// If the lock is not applied, it is still possible to read incorrect data.
		// For example, the shard exists when first check,
		// but the cluster starts to transfer shards and deletes the data quickly, the value read is an empty string
		shardMu.Lock()
		if _, exist = s.shards[shard]; exist {
			value, exist = s.ht[key]
			if !exist {
				errcode = shardproto.ResponseCode_KEY_NOT_EXISTS
			}
		} else {
			errcode = shardproto.ResponseCode_WRONG_GROUP
		}
		shardMu.Unlock()
	}
	errmsg := errcode.String()
	reply.Value = value
	reply.Exist = exist
	reply.ErrCode = errcode
	reply.ErrMessage = errmsg
	s.logPrintf("Get", "query key=%s, value=%s, exist=%v, errcode=%d, errerrmsg=%s", key, value, exist, errcode, errmsg)
	return reply, nil
}

func (s *server) Set(_ context.Context, args *shardproto.SetRequest) (*shardproto.SetReply, error) {
	key := args.Key
	value := args.Value
	c := command{
		CommandType: commandType_Set,
		Key:         key,
		Value:       value,
	}
	reply := &shardproto.SetReply{}
	errcode := s.submit(c)
	errmsg := errcode.String()
	reply.ErrCode = errcode
	reply.ErrMessage = errmsg
	s.logPrintf("Set", "set key=%s, value=%s, errcode=%d, errerrmsg=%s", key, value, errcode, errmsg)
	return reply, nil
}

func (s *server) Delete(_ context.Context, args *shardproto.DeleteRequest) (*shardproto.DeleteReply, error) {
	key := args.Key
	c := command{
		CommandType: commandType_Delete,
		Key:         key,
	}
	reply := &shardproto.DeleteReply{}
	errcode := s.submit(c)
	errmsg := errcode.String()
	reply.ErrCode = errcode
	reply.ErrMessage = errmsg
	s.logPrintf("Delete", "set key=%s, errcode=%d, errerrmsg=%s", key, errcode, errmsg)
	return reply, nil
}

func (s *server) Transfer(_ context.Context, args *shardproto.TransferRequest) (*shardproto.TransferReply, error) {
	c := command{
		CommandType: commandType_Transfer_Shards,
		ConfigId:    args.ConfigId,
		Shards:      args.Shards,
		Data:        args.Data,
	}
	reply := &shardproto.TransferReply{}
	reply.ErrCode = s.submit(c)
	reply.ErrMessage = reply.ErrCode.String()
	return reply, nil
}

func (s *server) configure(ownShards map[int]byte, currentGroup int32, configId int32, shardsMap []int32, replicaGroups map[int32][]string) {
	shardsToTransfer := make(map[int32][]int)
	shardsToGet := make(map[int]byte)
	for shard, ownGroup := range shardsMap {
		if _, own := ownShards[shard]; own {
			if ownGroup != currentGroup {
				shards := shardsToTransfer[ownGroup]
				shards = append(shards, shard)
				shardsToTransfer[ownGroup] = shards
			}
		} else {
			if ownGroup == currentGroup {
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
	responseCode := s.submit(c)
	s.logPrintf("configure", "submit configId:%d, responseCode=%s", configId, responseCode.String())
}

func (s *server) deleteShards(configId int32, groupId int32, keys []string) bool {
	c := command{
		CommandType: commandType_Delete_Shards,
		ConfigId:    configId,
		GroupId:     groupId,
		Keys:        keys,
	}
	errCode := s.submit(c)
	return errCode == shardproto.ResponseCode_OK
}

func (s *server) submit(command command) shardproto.ResponseCode {
	commandIndex, commandTerm, isLeader := s.rf.Start(command)
	if !isLeader {
		s.logPrintf("submit", "not leader, refuse command:%s", command.CommandString())
		return shardproto.ResponseCode_WRONG_LEADER
	}
	// leader1(current leader) may be partitioned by itself,
	// its log may be trimmed by leader2 (if and only if leader2's term > leader1's term)
	// but len(leader2's log) may less than len(leader1's log)
	// if leader1 becomes leader again, then commands submitted later may get the same log index
	// that's to say, previously submitted commands will never be completed

	// channel in go is implemented through wait list and ring buffer
	ch := make(chan int, 1)
	s.channelMu.Lock()
	if c, exist := s.waitingClients[commandIndex]; exist && c != nil {
		// tell the previous client to stop waiting
		c <- commandTerm
		close(c)
	}
	s.waitingClients[commandIndex] = ch
	s.channelMu.Unlock()
	s.logPrintf("submit", "waiting command to finish, commandIndex=%d, commandTerm=%d, command:%s", commandIndex, commandTerm, command.CommandString())

	receivedTerm := <-ch

	s.logPrintf("submit", "expected commandTerm=%d for commandIndex=%d, received commandTerm=%d", commandTerm, commandIndex, receivedTerm)
	// log's index and term identifies the unique log
	if receivedTerm == commandTerm {
		return shardproto.ResponseCode_OK
	}
	switch receivedTerm {
	case wrongGroupTerm:
		return shardproto.ResponseCode_WRONG_GROUP
	case configMismatchTerm:
		return shardproto.ResponseCode_CONFIG_MISMATCH
	default:
		return shardproto.ResponseCode_WRONG_LEADER
	}
}

func (s *server) listenConfig(routerClient *router.ListenerClient) {
	shardMu := s.shardMu
	groupId := s.groupId
	for {
		if !s.isLeader() {
			time.Sleep(1 * time.Second)
		}
		shardMu.Lock()
		reshard := s.reshard
		nextConfigId := s.currentConfigId + 1
		shardMu.Unlock()
		if !reshard {
			config, err := routerClient.Query(nextConfigId)
			if err != nil {
				//s.logPrintf("listenConfig", "query fail: %v", err)
			} else {
				//s.logPrintf("listenConfig", "get config: %v", config)
				if config.ConfigId == nextConfigId {
					shardMu.Lock()
					if s.currentConfigId+1 != nextConfigId {
						shardMu.Unlock()
					} else {
						shards := copyShards(s.shards)
						shardMu.Unlock()
						replicaGroups := config.ReplicaGroups
						s.configure(shards, groupId, config.ConfigId, config.ShardsMap, replicaGroups)
					}
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func copyShards(src map[int]byte) map[int]byte {
	dst := make(map[int]byte)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func (s *server) transferShards() {
	shardMu := s.shardMu
	cond := s.transferCond
	for {
		if !s.isLeader() {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		shardMu.Lock()
		for len(s.shardsToTransfer) == 0 && s.isLeader() {
			cond.Wait()
		}
		if !s.isLeader() {
			shardMu.Unlock()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		configId := s.currentConfigId
		// copy the shards that need to be transferred currently
		// in order to avoid holding the lock continuously during the data transfer process.
		shardsToMove := copyShardsToTransfer(s.shardsToTransfer)
		shardMu.Unlock()

		finished := true
		for gid, shards := range shardsToMove {
			if len(shards) == 0 {
				s.logFatalf("transferShards", "transfer empty shards to group:%d", gid)
			}

			// If there is a configuration change during the data transfer process,
			// it indicates that other nodes have successfully transferred the data.
			shardMu.Lock()
			if configId != s.currentConfigId {
				finished = false
				shardMu.Unlock()
				break
			}
			if hasTransferred(gid, s.shardsToTransfer) {
				shardMu.Unlock()
			} else {
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

				cancel := false
				if len(addrs) > 0 {
					client := MakeDataTransferClient(addrs)
					int32Shards := make([]int32, len(shards))
					for i, shard := range shards {
						int32Shards[i] = int32(shard)
					}
					if !s.isLeader() {
						finished = false
						time.Sleep(500 * time.Millisecond)
						break
					}

					shardMu.Lock()
					if s.currentConfigId != configId {
						shardMu.Unlock()
						finished = false
						break
					}
					shardMu.Unlock()

					err := client.Transfer(configId, int32Shards, data)
					for err != nil {
						time.Sleep(300 * time.Millisecond)
						if !s.isLeader() {
							cancel = true
							time.Sleep(500 * time.Millisecond)
							break
						}

						shardMu.Lock()
						if s.currentConfigId != configId {
							shardMu.Unlock()
							cancel = true
							break
						}
						shardMu.Unlock()

						err = client.Transfer(configId, int32Shards, data)
					}
				} // else... no groups alive,

				if cancel {
					finished = false
					break
				}

				// It is possible for failures to occur during the data transfer process,
				// so the data should not be deleted until the transfer is successful.
				finished = s.deleteShards(configId, gid, keys)
				if !finished {
					break
				}
			}
		}

		if finished {
			s.logPrintf("dataTransfer", "finish current move, switch to next")
		}
	}
}

func hasTransferred(groupToTransfer int32, shardsToTransfer map[int32][]int) bool {
	if _, needTransfer := shardsToTransfer[groupToTransfer]; needTransfer {
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
				s.logFatalf("executeCommands", "expected log index is %d, but received is %d, applyMsg=%v", expectedIndex, commandIndex, msg)
			}
			expectedIndex++
			c := msg.Command
			s.logPrintf("executeCommands", "receive committed command:%s", c.CommandString())
			ct := c.CommandType
			commandTerm := msg.CommandTerm
			if ct != commandType_Get {
				switch ct {
				case commandType_Set, commandType_Delete:
					ok := s.handleClientUpdateRequest(ct, c.Key, c.Value)
					if !ok {
						commandTerm = wrongGroupTerm
					}
					break
				case commandType_Configure:
					s.handleConfig(c.ConfigId, c.ShardsToGet, c.ShardsToTransfer, c.ReplicaGroups)
					break
				case commandType_Transfer_Shards:
					ok := s.handleShardsTransfer(c.ConfigId, c.Shards, c.Data)
					if !ok {
						commandTerm = configMismatchTerm
					}
					break
				case commandType_Delete_Shards:
					s.handleShardsDelete(c.ConfigId, c.GroupId, c.Keys)
					break
				default:
					s.logFatalf("receives unknown type command:%s", s.shardkvId, c.CommandString())
				}
			}
			s.replyClientIfNeeded(commandIndex, commandTerm)
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
				s.logFatalf("executeCommands", "service snapshot index is %d, but Raft snapshot index is %d", snapshotIndex, msg.SnapshotIndex)
			}
			if snapshotIndex < expectedIndex {
				s.logFatalf("executeCommands", "received snapshot is out-dated, expected log index=%d, received snapshot index=%d", expectedIndex, snapshotIndex)
			}
			expectedIndex = snapshotIndex + 1
		} else {
			s.logFatalf("executeCommands", "receive unknown type message from Raft, applyMsg=%v", msg)
		}
	}
}

func (s *server) replyClientIfNeeded(commandIndex int, commandTerm int) {
	s.channelMu.Lock()
	if ch, _ := s.waitingClients[commandIndex]; ch != nil {
		s.logPrintf("executeCommands", "send commandTerm=%d for commandIndex=%d", commandTerm, commandIndex)
		ch <- commandTerm
		close(ch)
		delete(s.waitingClients, commandIndex)
	}
	s.channelMu.Unlock()
}

// The transfer of shards and modification of data is single-threaded.
// The listenConfig and transferShards threads only perform read operations on this data.
// Therefore, locking can be deferred until just before modifying the data.
func (s *server) handleClientUpdateRequest(ct commandType, key string, value string) bool {
	shardMu := s.shardMu
	shard := router.Key2Shard(key)
	if _, exist := s.shards[shard]; exist {
		shardMu.Lock()
		if ct == commandType_Set {
			s.ht[key] = value
			s.logPrintf("handleClientUpdateRequest", "set %s -> %s", key, value)
		} else {
			delete(s.ht, key)
			s.logPrintf("handleClientUpdateRequest", "delete key %s", key)
		}
		shardMu.Unlock()
	} else {
		s.logPrintf("handleClientUpdateRequest", "wrong group, commandType=%v, key=%s, shard=%d, "+
			"current group is responsible for shards:%v", ct, key, shard, s.shards)
		return false
	}
	return true
}

func (s *server) handleConfig(configId int32, shardsToGet map[int]byte, shardsToTransfer map[int32][]int, replicaGroups map[int32][]string) {
	nextConfigId := s.currentConfigId + 1
	if configId > nextConfigId {
		s.logFatalf("[%s] expected nextConfigId=%d, but received configId=%d "+s.shardInfo(), s.shardkvId, nextConfigId, configId)
	} else if configId == nextConfigId {
		if !s.reshard {
			shardMu := s.shardMu
			shardMu.Lock()
			defer shardMu.Unlock()
			shards := s.shards
			if len(shardsToGet) != 0 || len(shardsToTransfer) != 0 {
				if s.noGroupsAliveBefore {
					// There are no existing shards before this, so no need to wait for other groups to transfer shards.
					for shard := range shardsToGet {
						shards[shard] = 1
					}
				} else {
					s.reshard = true
					for _, shardsToMove := range shardsToTransfer {
						for _, shard := range shardsToMove {
							delete(shards, shard)
						}
					}
					s.shardsToTransfer = shardsToTransfer
					s.shardsToGet = shardsToGet
					s.replicaGroups = replicaGroups
					// transfer shards if needed
					if len(shardsToTransfer) != 0 {
						s.transferCond.Signal()
					}
				}
			}
			s.noGroupsAliveBefore = len(replicaGroups) == 0
			s.logPrintf("handleConfig", "switch to next configuration, configId=%d, ", configId)
			s.logPrintf("handleConfig", s.shardInfo())
			s.currentConfigId = configId
		}

	} // else... stale configuration
}

func (s *server) handleShardsTransfer(configId int32, transferredShards []int32, data map[string]string) bool {
	if configId > s.currentConfigId {
		s.logPrintf("handleShardsTransfer", "refuse shards transfer for configId=%d, currentConfigId=%d", configId, s.currentConfigId)
		return false
	}
	if configId == s.currentConfigId {
		duplicate := false
		shards := s.shards
		for _, shard := range transferredShards {
			if _, own := shards[int(shard)]; own {
				duplicate = true
				break
			}
		}
		if !duplicate {
			shardMu := s.shardMu
			shardMu.Lock()
			// save data first, then shards...
			for k, v := range data {
				s.ht[k] = v
			}
			for _, shard := range transferredShards {
				shards[int(shard)] = 1
				delete(s.shardsToGet, int(shard))
			}
			s.logPrintf("handleShardsTransfer", "accepts data for shards:%v in configId:%d", transferredShards, configId)

			if len(s.shardsToTransfer) == 0 && len(s.shardsToGet) == 0 {
				s.logPrintf("handleShardsTransfer", "finishes reshard in configId=%d", configId)
				s.reshard = false
			}
			s.logPrintf("handleShardsTransfer", s.shardInfo())
			shardMu.Unlock()
		}

	} // else... stale transfer, return ok
	return true
}

func (s *server) handleShardsDelete(configId int32, transferredGroupId int32, keys []string) {
	if configId > s.currentConfigId {
		s.logFatalf("handleShardsDelete", "advanced shards delete, currentConfigId=%d, delete shards for configId=%d",
			s.currentConfigId, configId)
	} else if configId == s.currentConfigId {
		if shards, exist := s.shardsToTransfer[transferredGroupId]; exist {
			shardMu := s.shardMu
			shardMu.Lock()
			defer shardMu.Unlock()
			delete(s.shardsToTransfer, transferredGroupId)
			for _, key := range keys {
				delete(s.ht, key)
			}
			s.logPrintf("handleShardsDelete", "delete shards:%v for configId=%d", shards, configId)
			s.logPrintf("handleShardsDelete", s.shardInfo())
			if len(s.shardsToTransfer) == 0 && len(s.shardsToGet) == 0 {
				s.reshard = false
			}
		}
	} // else... stale delete
}
