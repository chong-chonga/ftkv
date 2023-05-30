package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ftkv/v1/raft"
	"github.com/ftkv/v1/router/routerproto"
	"github.com/ftkv/v1/storage"
	"github.com/ftkv/v1/tool"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
	"log"
	"net"
	"sort"
	"strconv"
)
import "sync"

type server struct {
	routerproto.RouterServer

	channelMu        sync.Mutex
	logEnabled       bool
	maxRaftStateSize int
	rf               *Raft
	applyCh          chan applyMsg
	storage          *storage.Storage
	waitingClients   map[int]chan commitResult

	routerId string
	// persistent
	snapshotIndex int
	configs       []ClusterConfig
	shardsMap     []int32
	replicaGroups map[int32][]string
}

type ServerConfigWrapper struct {
	ServerConfig ServerConfig `yaml:"routerServer"`
}

type ServerConfig struct {
	RouterId         string      `yaml:"routerId"`
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
	if len(serverConfig.RouterId) == 0 {
		return nil, errors.New("routerId is not configured")
	}
	return serverConfig, nil
}

type StopFunc func()

//
// StartRouterServer starts a router server which manages cluster configuration and data-sharding of the system.
//
func StartRouterServer(configData []byte) (string, StopFunc, error) {
	config, err := readServerConfig(configData)
	if err != nil {
		return "", nil, &tool.RuntimeError{Stage: "load config", Err: err}
	}

	port := config.Port
	if port <= 0 {
		return "", nil, &tool.RuntimeError{Stage: "configure router server", Err: errors.New("server port " + strconv.Itoa(port) + " is invalid")}
	}
	routerId := config.RouterId
	serviceName := "router-" + routerId
	storage, err := storage.MakeStorage(serviceName)
	if err != nil {
		return "", nil, &tool.RuntimeError{Stage: "make storage", Err: err}
	}

	rt := new(server)
	snapshot := storage.GetSnapshot()
	if nil != snapshot && len(snapshot) > 0 {
		if err = rt.restore(snapshot); err != nil {
			return "", nil, &tool.RuntimeError{Stage: "restore", Err: err}
		}
	} else {
		rt.initPersistentState()
	}
	rt.routerId = routerId

	rt.storage = storage
	rt.waitingClients = make(map[int]chan commitResult)

	// configure router
	maxRaftStateSize := config.MaxRaftStateSize
	rt.maxRaftStateSize = maxRaftStateSize
	if maxRaftStateSize > 0 {
		log.Printf("configure router info: service will make a snapshot when Raft state size bigger than %d bytes", maxRaftStateSize)
	} else {
		log.Println("configure router info: disable snapshot")
	}

	if config.LogEnabled {
		rt.logEnabled = true
		log.Println("configure router info: enable service log")
	} else {
		log.Println("configure router info: disable service log")
	}

	// start listener
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		err = &tool.RuntimeError{Stage: "start listener", Err: err}
		return "", nil, err
	}

	applyCh := make(chan applyMsg)
	rt.applyCh = applyCh
	// initialize success, start Raft
	raftConfig := config.Raft
	rt.rf, err = startRaft(raftConfig, storage, applyCh)
	if err != nil {
		_ = listener.Close()
		return "", nil, err
	}

	go rt.executeCommands()

	// start grpc server
	s := grpc.NewServer()
	routerproto.RegisterRouterServer(s, rt)
	go func() {
		_ = s.Serve(listener)
	}()

	log.Printf("start router server success, serviceName=%s, serves at port:%d", serviceName, port)
	return serviceName, s.Stop, nil
}

func (s *server) initPersistentState() {
	s.shardsMap = make([]int32, shardsCount)
	s.replicaGroups = make(map[int32][]string)
	s.snapshotIndex = 0
	s.makeNewConfig()
}

func (s *server) logPrintf(printInfo bool, method string, format string, a ...interface{}) {
	if s.logEnabled {
		str := fmt.Sprintf("["+(s.routerId)+"] "+method+": "+format, a...)
		if printInfo {
			str += ", "
			str += fmt.Sprintf("shardCount=%d, replicaGroups=%v", len(s.shardsMap), s.replicaGroups)
			shardGroups := s.shardGroups()

			str += "\n################################shards info start################################"
			if len(shardGroups) > 0 {
				for _, group := range shardGroups {
					str += fmt.Sprintf("\nshards:%v -> group:%d:%v", group.shards, group.gid, s.replicaGroups[group.gid])
				}
			} else {
				str += fmt.Sprintf("\nno alive groups")
			}
			str += "\n#################################shards info end#################################"

		}
		log.Println(str)
	}
}

func (s *server) makeSnapshot() (error, []byte) {
	w := new(bytes.Buffer)
	e := json.NewEncoder(w)
	configs := s.configs
	var err error
	if err = e.Encode(configs); err != nil {
		return errors.New("encode configs fails: " + err.Error()), nil
	}
	shardsMap := s.shardsMap
	if err = e.Encode(shardsMap); err != nil {
		return errors.New("encode shardMap fails: " + err.Error()), nil
	}
	replicaGroups := s.replicaGroups
	if err = e.Encode(replicaGroups); err != nil {
		return errors.New("encode raftGroups fails: " + err.Error()), nil
	}
	snapshotIndex := s.snapshotIndex
	if err = e.Encode(snapshotIndex); err != nil {
		return errors.New("encode snapshotIndex fails: " + err.Error()), nil
	}
	s.logPrintf(true, "makeSnapshot:", "has made a snapshot, snapshotIndex=%d", snapshotIndex)
	return nil, w.Bytes()
}

func (s *server) restore(snapshot []byte) error {
	if len(snapshot) == 0 {
		return errors.New("snapshot is nil")
	}
	r := bytes.NewBuffer(snapshot)
	d := json.NewDecoder(r)
	var configs []ClusterConfig
	var shardsMap []int32
	var replicaGroups map[int32][]string
	var snapshotIndex int
	var err error
	if err = d.Decode(&configs); err != nil {
		return errors.New("decode configs fails: " + err.Error())
	}
	if err = d.Decode(&shardsMap); err != nil {
		return errors.New("decode shardsMap fails: " + err.Error())
	}
	if err = d.Decode(&replicaGroups); err != nil {
		return errors.New("decode replicaGroups fails: " + err.Error())
	}
	if err = d.Decode(&snapshotIndex); err != nil {
		return errors.New("decode snapshotIndex fails: " + err.Error())
	}
	s.configs = configs
	s.shardsMap = shardsMap
	s.replicaGroups = replicaGroups
	s.snapshotIndex = snapshotIndex
	s.logPrintf(true, "restore", "restore snapshot success, snapshotIndex=%d", snapshotIndex)
	return nil
}

type commitResult struct {
	term   int
	result *ClusterConfig
}

// Query queries the ClusterConfig corresponding to the number.
// Its argument is a configuration number.
// If the number is -1 or bigger than the biggest known configuration number, the server replies with the latest configuration.
// The result of Query(-1) will reflect every Join, Leave, or Move RPC that the server finished handling before it received the Query(-1) RPC.
func (s *server) Query(_ context.Context, args *routerproto.QueryRequest) (*routerproto.QueryReply, error) {
	reply := &routerproto.QueryReply{}

	configId := args.ConfigId
	if ok, errcode, errmsg := routerproto.ValidateConfigId(configId); !ok {
		reply.ErrCode = errcode
		reply.ErrMessage = errmsg
		return reply, nil
	}

	op := command{
		CommandType: commandType_Query,
		ConfigId:    args.ConfigId,
	}

	errCode, queryRes := s.submit(op)

	if errCode == routerproto.ErrCode_ERR_CODE_OK {
		replicaGroups := make(map[int32]*routerproto.Servers)
		for gid, servers := range queryRes.ReplicaGroups {
			replicaGroups[gid] = &routerproto.Servers{
				Servers: servers,
			}
		}
		reply.ConfigWrapper = &routerproto.ClusterConfigWrapper{
			ConfigId:      queryRes.ConfigId,
			ShardsMap:     queryRes.ShardsMap,
			ReplicaGroups: replicaGroups,
		}
	}
	reply.ErrCode = errCode
	reply.ErrMessage = errCode.String()
	return reply, nil
}

// Join add new replica groups and re-sharding.
// Its argument is a set of mappings from unique, non-zero replica group identifiers (GIDs) to lists of server names.
// server will move as few shards as possible to divide the shards as evenly as possible among the groups and create a new ClusterConfig.
// server allow re-use of a GID if it's not part of the current configuration (i.e. a GID should be allowed to Join, then Leave, then Join again).
// If the added GIDs already exists, it will overwrite the current replica groups.
func (s *server) Join(_ context.Context, args *routerproto.JoinRequest) (*routerproto.JoinReply, error) {
	reply := &routerproto.JoinReply{}

	replicaGroups := make(map[int32][]string)
	for groupId, serverGroup := range args.ReplicaGroups {
		replicaGroups[groupId] = serverGroup.GetServers()
	}
	if ok, errcode, errmsg := routerproto.ValidateReplicaGroups(replicaGroups); !ok {
		reply.ErrCode = errcode
		reply.ErrMessage = errmsg
		return reply, nil
	}
	op := command{
		CommandType:   commandType_Join,
		ReplicaGroups: replicaGroups,
	}

	reply.ErrCode, _ = s.submit(op)
	reply.ErrMessage = reply.ErrCode.String()
	return reply, nil
}

// Leave remove replica groups and re-sharding.
// Its argument is a list of GIDs of previously joined groups.
// server will move as few shards as possible to divide the shards as evenly as possible among the groups and create a new ClusterConfig.
// If the specified GIDs does not exist, no action will be taken.
// If there are no remaining groups, then all the shards are assigned to GID0.
func (s *server) Leave(_ context.Context, args *routerproto.LeaveRequest) (*routerproto.LeaveReply, error) {
	reply := &routerproto.LeaveReply{}
	groupIds := args.GroupIds
	if ok, errcode, errmsg := routerproto.ValidateGroupIds(groupIds); !ok {
		reply.ErrCode = errcode
		reply.ErrMessage = errmsg
		return reply, nil
	}
	op := command{
		CommandType: commandType_Leave,
		GroupIds:    groupIds,
	}

	reply.ErrCode, _ = s.submit(op)
	reply.ErrMessage = reply.ErrCode.String()
	return reply, nil
}

func (s *server) submit(command command) (routerproto.ErrCode, *ClusterConfig) {
	commandIndex, commandTerm, isLeader := s.rf.Start(command)
	if !isLeader {
		s.logPrintf(false, "submit", "not leader, refuse command:%s", command.CommandString())
		return routerproto.ErrCode_ERR_CODE_WRONG_LEADER, nil
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
	s.logPrintf(false, "submit", "waiting command to finish, commandIndex=%d, commandTerm=%d, command:%s", commandIndex, commandTerm, command.CommandString())

	res := <-ch

	receivedTerm := res.term
	s.logPrintf(false, "submit", "expected commandTerm=%d for commandIndex=%d, received commandTerm=%d, config=%v", commandTerm, commandIndex, receivedTerm, res.result)
	// log's index and term identifies the unique log
	if receivedTerm == commandTerm {
		return routerproto.ErrCode_ERR_CODE_OK, res.result
	} else {
		return routerproto.ErrCode_ERR_CODE_WRONG_LEADER, nil
	}
}

type shardGroup struct {
	gid    int32
	shards []int
}

type byShardCount []shardGroup

// for sorting by shard count.
func (a byShardCount) Len() int      { return len(a) }
func (a byShardCount) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byShardCount) Less(i, j int) bool {
	// shard rebalancing needs to be deterministic.
	return len(a[i].shards) < len(a[j].shards) || (len(a[i].shards) == len(a[j].shards) && a[i].gid < a[j].gid)
}

// shardGroups returns empty array if there are no groups
// or else returns shardGroup array which is sorted by the number of shards included in ascending order.
// If two groups has the same number of shards, then sort by group id in ascending order.
// For the same sharding, the sorting result of this method is deterministic.
func (s *server) shardGroups() []shardGroup {
	if len(s.replicaGroups) == 0 {
		return make([]shardGroup, 0)
	}
	// count shards for each Raft group
	shardsMap := make(map[int32][]int)
	for gid := range s.replicaGroups {
		shardsMap[gid] = make([]int, 0)
	}
	for shard, gid := range s.shardsMap {
		shardsForG, groupExist := shardsMap[gid]
		if groupExist {
			shardsMap[gid] = append(shardsForG, shard)
		}
	}

	var shardGroups byShardCount
	for gid, shards := range shardsMap {
		shardGroups = append(shardGroups, shardGroup{gid: gid, shards: shards})
	}
	sort.Sort(shardGroups)
	return shardGroups
}

func (s *server) balanceJoin(groups map[int32][]string) bool {
	if len(groups) == 0 {
		s.logPrintf(false, "balanceJoin", "empty groups!")
		return false
	}
	var groupsToDistribute []int
	var overwriteGroups []int32
	for gid, joinServers := range groups {
		if curServers, exist := s.replicaGroups[gid]; !exist {
			groupsToDistribute = append(groupsToDistribute, int(gid))
		} else if !sameServers(joinServers, curServers) {
			overwriteGroups = append(overwriteGroups, gid)
		}
	}
	groupsToJoin := len(groupsToDistribute)
	if groupsToJoin == 0 && len(overwriteGroups) == 0 {
		s.logPrintf(true, "balanceJoin", "groups:%v already exist!", groups)
		return false
	}

	var shardsToDistribute []int
	groupsBeforeJoin := len(s.replicaGroups)
	groupsAfterJoin := groupsToJoin + groupsBeforeJoin

	// only re-sharding if the number of groups changes
	if groupsToJoin != 0 {
		sort.Ints(groupsToDistribute)
		if groupsBeforeJoin == 0 || groupsBeforeJoin < shardsCount {
			if groupsBeforeJoin == 0 {
				shardsToDistribute = allShards(shardsCount)
			} else {
				shardGroups := s.shardGroups()
				avg := shardsCount / groupsAfterJoin
				mod := shardsCount % groupsAfterJoin
				for i := len(shardGroups) - 1; i >= 0; i-- {
					shards := shardGroups[i].shards
					n := len(shards)
					if n <= 1 {
						break
					}
					var distribute int
					// if shards <= groups, re-shard n-1 shards per group(n > 1)，
					// else, distribute n - avg shards per group，However, there are mod shards that do not need to be re-sharded.
					if avg <= 1 {
						distribute = n - 1
					} else {
						distribute = n - avg
						if mod > 0 {
							distribute--
							mod--
						}
					}
					for j := 0; j < distribute; j++ {
						shardsToDistribute = append(shardsToDistribute, shards[j])
					}
				}
			}
		}
	}
	s.logPrintf(true, "balanceJoin", "there are %d groups before join groups:%v", groupsBeforeJoin, groups)
	if len(shardsToDistribute) > 0 {
		s.distributeShards(shardsToDistribute, groupsToDistribute)
	}
	s.logPrintf(false, "balanceJoin", "new groups:%v, overwrite groups:%v", groupsToDistribute, overwriteGroups)

	// add or overwrite groups
	for gid, servers := range groups {
		s.replicaGroups[gid] = servers
	}
	s.logPrintf(true, "balanceJoin", "there are %d groups after join groups:%v", groupsAfterJoin, groups)
	return true
}

func sameServers(servers1 []string, servers2 []string) bool {
	if len(servers1) != len(servers2) {
		return false
	}
	for i := range servers1 {
		if servers1[i] != servers2[i] {
			return false
		}
	}
	return true
}

func (s *server) balanceLeave(groupIds []int32) bool {
	if len(groupIds) == 0 {
		s.logPrintf(false, "balanceLeave", "empty group ids")
		return false
	}
	leaveIdMap := make(map[int32]byte)
	var actualLeaveGroups []int32
	for _, gid := range groupIds {
		if _, exist := s.replicaGroups[gid]; exist {
			leaveIdMap[gid] = 1
			actualLeaveGroups = append(actualLeaveGroups, gid)
		}
	}

	groupsToLeave := len(actualLeaveGroups)
	if groupsToLeave == 0 {
		s.logPrintf(false, "balanceLeave", "groupIds:%v not exist!", groupIds)
		return false
	}

	groupsBeforeLeave := len(s.replicaGroups)
	groupsAfterLeave := groupsBeforeLeave - groupsToLeave
	// groups to receive shards from left groups
	var groupsToDistribute []int
	var shardsToDistribute []int

	s.logPrintf(true, "balanceLeave", "there are %d groups before leave groups:%v", groupsBeforeLeave, groupIds)

	if groupsAfterLeave == 0 {
		// no groups alive, clear shardsMap
		for i := range s.shardsMap {
			s.shardsMap[i] = 0
		}
	} else {
		shardGroups := s.shardGroups()
		for _, group := range shardGroups {
			gid := group.gid
			// if a group leaves, add its shards to shardsToDistribute
			// else add group id to groupsToDistribute
			if _, inLeaveGroup := leaveIdMap[gid]; inLeaveGroup {
				if len(group.shards) > 0 {
					shardsToDistribute = append(shardsToDistribute, group.shards...)
				}
			} else {
				groupsToDistribute = append(groupsToDistribute, int(gid))
			}
		}
	}
	if len(shardsToDistribute) > 0 {
		s.distributeShards(shardsToDistribute, groupsToDistribute)
	}
	s.logPrintf(false, "balanceLeave", "leave %v groups", actualLeaveGroups)
	// delete groups
	for _, gid := range actualLeaveGroups {
		delete(s.replicaGroups, gid)
	}
	s.logPrintf(true, "balanceLeave", "there are %d groups after leave groups:%v", groupsAfterLeave, groupIds)
	return true
}

func allShards(shardCount int) []int {
	shards := make([]int, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = i
	}
	return shards
}

// distributeShards distribute the given shards to the specified groups evenly
// groupsToDistribute: group ids, sort by shard counts, asc
func (s *server) distributeShards(shardsToDistribute []int, groupsToDistribute []int) {
	groups := len(groupsToDistribute)
	avg := len(shardsToDistribute) / groups
	mod := len(shardsToDistribute) % groups
	i := 0
	for _, gid := range groupsToDistribute {
		for j := 0; j < avg; j++ {
			s.shardsMap[shardsToDistribute[i]] = int32(gid)
			i++
		}
		if mod > 0 {
			s.shardsMap[shardsToDistribute[i]] = int32(gid)
			i++
			mod--
		}
	}
	s.logPrintf(false, "distributeShards", "distribute %d shards:%v to groups:%v", len(shardsToDistribute), shardsToDistribute, groupsToDistribute)
}

func (s *server) makeNewConfig() {
	shardsMapCopy := make([]int32, shardsCount)
	for i := range s.shardsMap {
		shardsMapCopy[i] = s.shardsMap[i]
	}
	replicaGroupsCopy := make(map[int32][]string)
	for gid, servers := range s.replicaGroups {
		serversCopy := make([]string, len(servers))
		for i, serverAddr := range servers {
			serversCopy[i] = serverAddr
		}
		replicaGroupsCopy[gid] = serversCopy
	}

	id := len(s.configs)
	c := ClusterConfig{
		ConfigId:      int32(id),
		ShardsMap:     shardsMapCopy,
		ReplicaGroups: replicaGroupsCopy,
	}

	s.configs = append(s.configs, c)
	s.logPrintf(false, "makeNewConfig", "make config:%d", id)
}

//// used for test
//func (sc *server) check() {
//	c := router.configs[len(router.configs)-1]
//	counts := map[int]int{}
//	for _, g := range c.Shards {
//		counts[g] += 1
//	}
//	min := 257
//	max := 0
//	for g := range c.Groups {
//		if counts[g] > max {
//			max = counts[g]
//		}
//		if counts[g] < min {
//			min = counts[g]
//		}
//	}
//	if max > min+1 {
//		log.Fatalln("[", router.routerId, "]", router.shardMap)
//	}
//}

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
				log.Fatalf("[%s] expected log inedx is %d, but received is %d, applyMsg=%v", s.routerId, expectedIndex, commandIndex, msg)
			}
			expectedIndex++
			c := msg.Command
			ct := c.CommandType
			s.logPrintf(false, "executeCommands", "receive committed command:%s", c.CommandString())
			var result *ClusterConfig = nil
			if ct == commandType_Query {
				configId := c.ConfigId
				idx := int(configId)
				// If the number is -1 or bigger than the biggest known configuration number, query the latest configuration.
				if configId == routerproto.LatestConfigId || int(configId) >= len(s.configs) {
					idx = len(s.configs) - 1
				}
				if idx >= 0 && idx < len(s.configs) {
					result = &s.configs[idx]
				}
			} else {
				var configChanges bool
				if commandType_Join == ct {
					configChanges = s.balanceJoin(c.ReplicaGroups)
				} else if commandType_Leave == ct {
					configChanges = s.balanceLeave(c.GroupIds)
				} else {
					log.Fatalf("[%s] receive unknown type operation, command:%s", s.routerId, c.CommandString())
				}
				if configChanges {
					s.makeNewConfig()
				}
			}
			//// used for test
			//if commandType_Join == commandType || commandType_Leave == commandType {
			//	s.check()
			//}
			s.replyClientIfNeeded(commandIndex, msg.CommandTerm, result)
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
				log.Fatalf("[%s] service snapshot index is %d, but Raft snapshot index is %d", s.routerId, snapshotIndex, msg.SnapshotIndex)
			}
			// 2023-05-26 bug：raft稳定通过测试的前提下，出现snapshotIndex < expectedIndex，且snapshotIndex=expectedIndex - 1。猜测是server
			//更新snapshotIndex 、expectedIndex逻辑有问题/raft install snapshot存在问题
			// 修改方案：
			// 1. 将service安装snapshot的条件修改为 snapshotIndex >= expectedIndex
			// 2. 将raft接受snapshot的条件修改为 snapshotIndex > lastApplied
			if snapshotIndex < expectedIndex {
				log.Fatalf("[%s] received snapshot is out-dated, expected log index=%d, received snapshot index=%d", s.routerId, expectedIndex, snapshotIndex)
			}
			expectedIndex = snapshotIndex + 1
		} else {
			log.Fatalf("[%s] receive unknown type meesage from Raft, applyMsg=%v", s.routerId, msg)
		}
	}
}

func (s *server) replyClientIfNeeded(commandIndex int, commandTerm int, result *ClusterConfig) {
	s.channelMu.Lock()
	if ch, _ := s.waitingClients[commandIndex]; ch != nil {
		s.logPrintf(false, "executeCommands", "send commandTerm=%d for commandIndex=%d, config=%v", commandTerm, commandIndex, result)
		ch <- commitResult{
			term:   commandTerm,
			result: result,
		}
		close(ch)
		delete(s.waitingClients, commandIndex)
	}
	s.channelMu.Unlock()
}
