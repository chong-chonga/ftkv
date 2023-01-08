package server

import (
	"bytes"
	"context"
	"google.golang.org/grpc"
	pb "kvraft/src/grpc/server/proto"
	"kvraft/src/grpc/server/raft"
	"kvraft/src/safegob"
	"kvraft/src/storage"
	"log"
	"net"
	"sync"
	"time"
)

const (
	Log3AEnabled      = false
	Log3BEnabled      = false
	LogSessionEnabled = false
)

//const RequestTimeout = time.Duration(300) * time.Millisecond

const SessionTimeout = 1 * time.Hour

const Token = "iqwcnksop19fakj129fsao)k12kfsj20-2jkfnak"

const DefaultMaxRaftState = 30

const (
	ClearSession pb.Op = -2
	OpenSession  pb.Op = -3
	GET          pb.Op = -4
)

type Op struct {
	OpType    pb.Op
	Key       string
	Value     string
	ClientId  int64 // to detect duplicate request
	RequestId int64 // to detect duplicate request
}

type ApplyResult struct {
	Term     int
	ClientId int64
}

type KVServer struct {
	pb.UnimplementedKVServerServer
	mu            sync.Mutex // big lock
	me            int
	maxRaftState  int
	cleanupIndex  int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	persister     *storage.Storage
	replyChan     map[int]chan ApplyResult
	sessionMap    map[int64]time.Time
	snapshotIndex int

	// persistent
	commitIndex  int
	nextClientId int64
	requestMap   map[int64]int64
	tab          map[string]string
}

//
// StartKVServer starts a key/value server for rpc call which using raft to keep consistency
//
func StartKVServer(serverAddress []string, raftAddresses []string, me int, storageFile *storage.Storage, maxRaftState int) *KVServer {
	// call safegob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	safegob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftState = maxRaftState
	kv.persister = storageFile
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.replyChan = make(map[int]chan ApplyResult)
	kv.sessionMap = make(map[int64]time.Time)
	snapshot := storageFile.ReadSnapshot()
	if nil != snapshot && len(snapshot) > 0 {
		kv.recoverFrom(snapshot)
	} else {
		kv.requestMap = make(map[int64]int64)
		kv.tab = make(map[string]string)
		kv.nextClientId = 0
		kv.commitIndex = 0
	}
	if maxRaftState > 0 {
		if maxRaftState < DefaultMaxRaftState {
			log.Println("info: the maxRaftState set is too small! set to default maxRaftState")
			maxRaftState = DefaultMaxRaftState
		}
		kv.snapshotIndex = kv.commitIndex + maxRaftState
	} else {
		kv.snapshotIndex = -1
	}

	kv.rf = raft.StartRaft(raftAddresses, me, storageFile, kv.applyCh)
	go kv.startApply()
	go kv.cleanupSessions()

	// start grpc server
	listener, _ := net.Listen("tcp", serverAddress[me])
	server := grpc.NewServer()
	pb.RegisterKVServerServer(server, kv)
	go func() {
		_ = server.Serve(listener)
	}()
	return kv
}

func log3A(format string, a ...interface{}) {
	if Log3AEnabled {
		log.Printf(format, a...)
	}
}

func log3B(format string, v ...interface{}) {
	if Log3BEnabled {
		log.Printf(format, v...)
	}
}

func logSession(format string, v ...interface{}) {
	if LogSessionEnabled {
		log.Printf(format, v...)
	}
}

func (kv *KVServer) ClearSession(_ context.Context, args *pb.ClearSessionRequest) (*pb.ClearSessionReply, error) {
	reply := &pb.ClearSessionReply{}
	if Token != args.Token {
		reply.ErrCode = pb.ErrCode_INVALID_TOKEN
		return reply, nil
	}
	command := Op{
		OpType:    ClearSession,
		Key:       "",
		Value:     "",
		ClientId:  -1,
		RequestId: -1,
	}
	_, errCode := kv.submit(command)
	if errCode == pb.ErrCode_OK {
		reply.ErrCode = pb.ErrCode_OK
	} else {
		reply.ErrCode = errCode
	}
	return reply, nil
}

func (kv *KVServer) OpenSession(_ context.Context, _ *pb.OpenSessionRequest) (*pb.OpenSessionReply, error) {
	reply := &pb.OpenSessionReply{}
	logSession("[%d] receive OpenSession request from client", kv.me)
	reply.ClientId = -1
	command := Op{
		OpType:    OpenSession,
		Key:       "",
		Value:     "",
		ClientId:  -1,
		RequestId: -1,
	}
	applyResult, errCode := kv.submit(command)
	if errCode == pb.ErrCode_OK {
		reply.ClientId = applyResult.ClientId
		reply.ErrCode = pb.ErrCode_OK
	} else {
		reply.ErrCode = errCode
	}
	return reply, nil
}

func (kv *KVServer) Get(_ context.Context, args *pb.GetRequest) (*pb.GetReply, error) {
	reply := &pb.GetReply{}
	log3A("[%d] receive get request from client, requestId=%d", kv.me, args.RequestId)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.ErrCode = pb.ErrCode_WRONG_LEADER
		return reply, nil
	}
	if !kv.isSessionValid(args.ClientId) {
		reply.ErrCode = pb.ErrCode_INVALID_SESSION
		return reply, nil
	}
	command := Op{
		OpType:    GET,
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	log3A("[%d] start requestId=%d", kv.me, args.RequestId)
	// start时，不应当持有锁，因为这会阻塞 KVServer 接收来自 raft 的 log
	// start的命令的返回值的唯一性已经由 raft 的互斥锁保证
	_, errCode := kv.submit(command)
	if errCode == pb.ErrCode_OK {
		kv.mu.Lock()
		if v, exist := kv.tab[args.Key]; !exist {
			reply.ErrCode = pb.ErrCode_NO_KEY
			reply.Value = ""
		} else {
			reply.ErrCode = pb.ErrCode_OK
			reply.Value = v
		}
		kv.mu.Unlock()
	} else {
		reply.ErrCode = errCode
	}

	return reply, nil
}

func (kv *KVServer) Update(_ context.Context, args *pb.UpdateRequest) (*pb.UpdateReply, error) {
	reply := &pb.UpdateReply{}
	log3A("[%d] receive %v request from client, requestId=%d", kv.me, args.Op, args.RequestId)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.ErrCode = pb.ErrCode_WRONG_LEADER
		return reply, nil
	}
	if !kv.isSessionValid(args.ClientId) {
		reply.ErrCode = pb.ErrCode_INVALID_SESSION
		return reply, nil
	}

	op := args.Op
	if op != pb.Op_PUT && op != pb.Op_APPEND && op != pb.Op_DELETE {
		reply.ErrCode = pb.ErrCode_INVALID_OP
		return reply, nil
	}
	command := Op{
		OpType:    op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	log3A("[%d] start requestId=%d", kv.me, args.RequestId)
	_, errCode := kv.submit(command)
	reply.ErrCode = errCode
	return reply, nil
}

func (kv *KVServer) isSessionValid(clientId int64) bool {
	kv.mu.Lock()
	if _, valid := kv.sessionMap[clientId]; valid {
		kv.sessionMap[clientId] = time.Now()
		kv.mu.Unlock()
		return true
	}
	kv.mu.Unlock()
	return false
}

// submit
// Submits an Op to Raft
// If the submitted command reaches consensus successfully, the pointer points to the corresponding ApplyResult will be returned;
// and the pb.ErrCode is pb.ErrCode_OK
// otherwise, the returned *ApplyResult would be nil and the ErrCode indicating the reason why the command fails to complete
func (kv *KVServer) submit(op Op) (*ApplyResult, pb.ErrCode) {
	commandIndex, commandTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil, pb.ErrCode_WRONG_LEADER
	}
	// leader1(current leader) may be partitioned by itself,
	// its log may be trimmed by leader2 (if and only if leader2's term > leader1's term)
	// but len(leader2's log) may less than len(leader1's log)
	// if leader1 becomes leader again, then commands submitted later may get the same log index
	// that's to say, previously submitted commands will never be completed
	kv.mu.Lock()
	if c, _ := kv.replyChan[commandIndex]; c != nil {
		// tell the previous client to stop waiting
		c <- ApplyResult{Term: commandTerm}
		close(c)
	}
	ch := make(chan ApplyResult, 1)
	kv.replyChan[commandIndex] = ch
	kv.mu.Unlock()

	res := <-ch
	// log's index and term identifies the unique log
	if res.Term == commandTerm {
		return &res, pb.ErrCode_OK
	} else {
		return nil, pb.ErrCode_WRONG_LEADER
	}
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := safegob.NewEncoder(w)
	if e.Encode(kv.nextClientId) != nil ||
		e.Encode(kv.commitIndex) != nil ||
		e.Encode(kv.tab) != nil ||
		e.Encode(kv.requestMap) != nil {
		log.Fatalf("[%d] encode snapshot failed!", kv.me)
	}
	log3B("[%d] encode snapshot success!", kv.me)
	return w.Bytes()
}

func (kv *KVServer) recoverFrom(snapshot []byte) {
	// bootstrap without snapshot ?
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := safegob.NewDecoder(r)
	var tab map[string]string
	var nextClientId int64
	var commitIndex int
	var requestMap map[int64]int64
	if d.Decode(&nextClientId) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&tab) != nil ||
		d.Decode(&requestMap) != nil {
		log.Fatalf("[%d] decode snapshot failed!", kv.me)
	}
	kv.nextClientId = nextClientId
	kv.commitIndex = commitIndex
	kv.tab = tab
	kv.requestMap = requestMap
	log3B("[%d] read from snapshot success!", kv.me)
}

// startApply listen to the log sent from applyCh and execute the corresponding command.
// The same command will only be executed once
func (kv *KVServer) startApply() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			commandType := op.OpType
			requestId := op.RequestId
			log3B("[%d] receive log, clientId=%d, requestId=%d, reqId=%d", kv.me, op.ClientId, op.RequestId, kv.requestMap[op.ClientId])
			result := ApplyResult{
				Term: msg.CommandTerm,
			}
			kv.mu.Lock()
			if ClearSession == commandType {
				kv.sessionMap = make(map[int64]time.Time)
				kv.requestMap = make(map[int64]int64)
				kv.nextClientId = 1
			} else if OpenSession == commandType {
				kv.sessionMap[kv.nextClientId] = time.Now()
				result.ClientId = kv.nextClientId
				kv.nextClientId++
				logSession("[%d] open a new session, clientId=%d", kv.me, result.ClientId)
			} else {
				if id, exists := kv.requestMap[op.ClientId]; !exists || requestId > id {
					if pb.Op_PUT == commandType {
						kv.tab[op.Key] = op.Value
						log3A("[%d] execute put %s on key=%v, clientId=%d,requestId=%d", kv.me, op.Value, op.Key, op.ClientId, op.RequestId)
					} else if pb.Op_APPEND == commandType {
						v := kv.tab[op.Key]
						v += op.Value
						kv.tab[op.Key] = v
						log3A("[%d] execute append %s on key=%v, now value is %s, clientId=%d,requestId=%d", kv.me, op.Value, op.Key, v, op.ClientId, op.RequestId)
					} else if pb.Op_DELETE == commandType {
						delete(kv.tab, op.Key)
						log3A("[%d execute delete on key=%v, clientId=%d,requestId=%d", kv.me, op.Key, op.ClientId, op.RequestId)
					} else if GET != commandType {
						log.Printf("[%d] receive unknown request type, requestId=%d,opType=%s\n", kv.me, op.RequestId, op.OpType)
					}
					kv.requestMap[op.ClientId] = requestId
				} else {
					log3A("[%d] duplicate %s request, requestId=%d", kv.me, op.OpType, op.RequestId)
				}
			}
			kv.commitIndex = msg.CommandIndex
			if ch, _ := kv.replyChan[kv.commitIndex]; ch != nil {
				log3A("[%d] send requestId=%d,commandIndex=%d, to ch", kv.me, op.RequestId, msg.CommandIndex)
				ch <- result
				close(ch)
				delete(kv.replyChan, kv.commitIndex)
				log3A("[%d] close requestId=%d ch", kv.me, op.RequestId)
			}
			if kv.maxRaftState > 0 && kv.commitIndex == kv.snapshotIndex {
				kv.snapshotIndex += kv.maxRaftState
				snapshot := kv.makeSnapshot()
				go kv.rf.Snapshot(kv.commitIndex, snapshot)
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			snapshot := msg.Snapshot
			lastIncludedIndex := msg.SnapshotIndex
			lastIncludedTerm := msg.SnapshotTerm
			go func(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) {
				if kv.rf.CondInstallSnapshot(lastIncludedTerm, lastIncludedIndex, snapshot) {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					kv.recoverFrom(snapshot)
				}
			}(lastIncludedTerm, lastIncludedIndex, snapshot)
		} else {
			log.Fatalf("[%d] receive unknown type log!", kv.me)
		}
	}
}

// cleanupSessions scans the requestMap to clean up the records corresponding to the expired clientId
func (kv *KVServer) cleanupSessions() {
	for {
		time.Sleep(SessionTimeout)
		kv.mu.Lock()
		var deleteArr []int64

		for clientId := range kv.sessionMap {
			lastVisitTime, visit := kv.sessionMap[clientId]
			if !visit || time.Since(lastVisitTime) >= SessionTimeout {
				deleteArr = append(deleteArr, clientId)
			}
		}
		for _, c := range deleteArr {
			delete(kv.sessionMap, c)
		}
		kv.mu.Unlock()
	}
}
