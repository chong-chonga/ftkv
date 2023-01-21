package kvserver

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/grpc"
	"kvraft/kvdb"
	"kvraft/raft"
	"kvraft/tool"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	Log3AEnabled      = true
	Log3BEnabled      = true
	LogSessionEnabled = true
)

const SessionTimeout = 1 * time.Hour

const DefaultMaxRaftState = 30

const SessionIdSeparator = "-"

const (
	OpenSession kvdb.Op = -2
	GET         kvdb.Op = -3
)

type Op struct {
	OpType kvdb.Op
	Key    string
	Value  string
	UUID   string
}

type ApplyResult struct {
	Term      int
	SessionId string
}

type KVServer struct {
	kvdb.KVServerServer
	mu                sync.Mutex // big lock
	me                int
	rf                *raft.Raft
	applyCh           chan raft.ApplyMsg
	storage           *tool.Storage
	replyChan         map[int]chan ApplyResult
	sessionMap        map[string]time.Time
	maxRaftState      int
	nextSnapshotIndex int
	password          string

	// persistent
	uniqueId    int64
	commitIndex int
	tab         map[string]string
}

//
// StartKVServer starts a key/value server for rpc call which using raft to keep consistency
//
func StartKVServer(serverPort int, me int, raftAddresses []string, raftPort int, storage *tool.Storage, maxRaftState int) (*KVServer, error) {
	gob.Register(Op{})
	servers := len(raftAddresses) + 1
	if servers&1 == 0 {
		return nil, &tool.RuntimeError{Stage: "start KVServer", Err: tool.ErrEvenServers}
	}
	if serverPort <= 0 || raftPort <= 0 {
		return nil, &tool.RuntimeError{Stage: "start KVServer", Err: tool.ErrInvalidPort}
	}
	if storage == nil {
		return nil, &tool.RuntimeError{Stage: "start KVServer", Err: tool.ErrNilStorage}
	}
	kv := new(KVServer)
	kv.me = me
	kv.storage = storage
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.replyChan = make(map[int]chan ApplyResult)
	kv.sessionMap = make(map[string]time.Time)
	kv.commitIndex = 0

	snapshot := storage.ReadSnapshot()
	if nil != snapshot && len(snapshot) > 0 {
		err := kv.recoverFrom(snapshot)
		if err != nil {
			return nil, &tool.RuntimeError{Stage: "start KVServer", Err: err}
		}
	} else {
		kv.tab = make(map[string]string)
		kv.uniqueId = 0
	}
	if maxRaftState > 0 {
		if maxRaftState < DefaultMaxRaftState {
			log.Println("start KVServer info: the maxRaftState set is too small! set to default maxRaftState")
			maxRaftState = DefaultMaxRaftState
		}
		kv.maxRaftState = maxRaftState
		kv.nextSnapshotIndex = kv.commitIndex + maxRaftState
	} else {
		kv.nextSnapshotIndex = -1
		kv.maxRaftState = -1
	}
	var err error
	// start grpc server
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(serverPort))
	if err != nil {
		err = &tool.RuntimeError{Stage: "start KVServer", Err: err}
		return nil, err
	}

	kv.rf, err = raft.StartRaft(raftAddresses, me, raftPort, storage, kv.applyCh)
	if err != nil {
		return nil, err
	}

	go kv.startApply()
	go kv.cleanupSessions()

	server := grpc.NewServer()
	kvdb.RegisterKVServerServer(server, kv)
	go func() {
		_ = server.Serve(listener)
	}()

	log.Println("start KVServer success, server port:", serverPort)
	return kv, nil
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

func (kv *KVServer) OpenSession(_ context.Context, request *kvdb.OpenSessionRequest) (*kvdb.OpenSessionReply, error) {
	reply := &kvdb.OpenSessionReply{}
	logSession("[%d] receive OpenSession request from client", kv.me)
	reply.SessionId = ""
	if request.GetPassword() != kv.password {
		reply.ErrCode = kvdb.ErrCode_INVALID_PASSWORD
		return reply, nil
	}
	command := Op{
		OpType: OpenSession,
		Key:    "",
		Value:  "",
		UUID:   uuid.NewV4().String(),
	}
	applyResult, errCode := kv.submit(command)
	if errCode == kvdb.ErrCode_OK {
		reply.SessionId = applyResult.SessionId
		reply.ErrCode = kvdb.ErrCode_OK
	} else {
		reply.ErrCode = errCode
	}
	return reply, nil
}

func (kv *KVServer) Get(_ context.Context, args *kvdb.GetRequest) (*kvdb.GetReply, error) {
	reply := &kvdb.GetReply{}
	log3A("[%d] receive get request from client, sessionId=%s", kv.me, args.SessionId)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.ErrCode = kvdb.ErrCode_WRONG_LEADER
		return reply, nil
	}
	if !kv.checkSession(args.SessionId) {
		reply.ErrCode = kvdb.ErrCode_INVALID_SESSION
		return reply, nil
	}
	command := Op{
		OpType: GET,
		Key:    args.Key,
		Value:  "",
	}
	log3A("[%d] start get request, sessionId=%s", kv.me, args.SessionId)

	_, errCode := kv.submit(command)
	if errCode == kvdb.ErrCode_OK {
		// 为提高读取时的性能，允许出现data race
		if v, exist := kv.tab[args.Key]; !exist {
			reply.ErrCode = kvdb.ErrCode_NO_KEY
			reply.Value = ""
		} else {
			reply.ErrCode = kvdb.ErrCode_OK
			reply.Value = v
		}
	} else {
		reply.ErrCode = errCode
	}

	return reply, nil
}

func (kv *KVServer) Update(_ context.Context, args *kvdb.UpdateRequest) (*kvdb.UpdateReply, error) {
	reply := &kvdb.UpdateReply{}
	log3A("[%d] receive %v request from client, sessionId=%s", kv.me, args.Op, args.SessionId)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.ErrCode = kvdb.ErrCode_WRONG_LEADER
		return reply, nil
	}
	if !kv.checkSession(args.SessionId) {
		reply.ErrCode = kvdb.ErrCode_INVALID_SESSION
		return reply, nil
	}

	op := args.Op
	if op != kvdb.Op_PUT && op != kvdb.Op_APPEND && op != kvdb.Op_DELETE {
		reply.ErrCode = kvdb.ErrCode_INVALID_OP
		return reply, nil
	}
	command := Op{
		OpType: op,
		Key:    args.Key,
		Value:  args.Value,
	}
	log3A("[%d] start update request, sessionId=%s", kv.me, args.SessionId)
	_, errCode := kv.submit(command)
	reply.ErrCode = errCode
	return reply, nil
}

func (kv *KVServer) checkSession(sessionId string) bool {
	kv.mu.Lock()
	if _, valid := kv.sessionMap[sessionId]; valid {
		kv.sessionMap[sessionId] = time.Now()
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
func (kv *KVServer) submit(op Op) (*ApplyResult, kvdb.ErrCode) {
	// start时，不应当持有锁，因为这会阻塞 KVServer 接收来自 raft 的 log
	// start的命令的返回值的唯一性已经由 raft 的互斥锁保证
	commandIndex, commandTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil, kvdb.ErrCode_WRONG_LEADER
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
		return &res, kvdb.ErrCode_OK
	} else {
		return nil, kvdb.ErrCode_WRONG_LEADER
	}
}

func (kv *KVServer) makeSnapshot() ([]byte, error) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	err := e.Encode(kv.uniqueId)
	if err != nil {
		return nil, errors.New("encode uniqueId fails: " + err.Error())
	}
	err = e.Encode(kv.commitIndex)
	if err != nil {
		return nil, errors.New("encode commitIndex fails: " + err.Error())
	}
	err = e.Encode(kv.tab)
	if err != nil {
		return nil, errors.New("encode tab fails: " + err.Error())
	}
	log3B("[%d] encode snapshot success!", kv.me)
	return w.Bytes(), nil
}

func (kv *KVServer) recoverFrom(snapshot []byte) error {
	if snapshot == nil || len(snapshot) < 1 {
		return nil
	}
	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)
	var tab map[string]string
	var nextClientId int64
	var commitIndex int
	var err error
	if err = d.Decode(&nextClientId); err != nil {
		return errors.New("recover from snapshot: decode currentTerm fails: " + err.Error())
	}
	if err = d.Decode(&commitIndex); err != nil {
		return errors.New("recover from snapshot: decode commitIndex fails: " + err.Error())
	}
	if err = d.Decode(&tab); err != nil {
		return errors.New("recover from snapshot: decode tab fails: " + err.Error())
	}
	kv.uniqueId = nextClientId
	kv.commitIndex = commitIndex
	kv.tab = tab
	log3B("[%d] read from snapshot success!", kv.me)
	return nil
}

// startApply listen to the log sent from applyCh and execute the corresponding command.
// The same command will only be executed once
func (kv *KVServer) startApply() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			commandIndex := msg.CommandIndex
			commandTerm := msg.CommandTerm
			log3B("[%d] receive log, commandIndex=%d,commandTerm=%d", kv.me, commandIndex, commandTerm)
			kv.mu.Lock()
			if commandIndex < kv.commitIndex {
				kv.mu.Unlock()
				log.Println(kv.me, "ignore out dated log, expected log index", kv.commitIndex+1, "but receive", commandIndex)
				continue
			}
			kv.commitIndex = commandIndex
			op := msg.Command.(Op)
			commandType := op.OpType
			sessionId := ""
			if OpenSession == commandType {
				sessionId = strconv.FormatInt(kv.uniqueId, 10) + SessionIdSeparator + op.UUID
				kv.sessionMap[sessionId] = time.Now()
				kv.uniqueId++
				logSession("[%d] open a new session, sessionId=%s", kv.me, sessionId)
			} else if kvdb.Op_PUT == commandType {
				kv.tab[op.Key] = op.Value
				log3A("[%d] execute put %s on key=%v, sessionId=%s", kv.me, op.Value, op.Key)
			} else if kvdb.Op_APPEND == commandType {
				v := kv.tab[op.Key]
				v += op.Value
				kv.tab[op.Key] = v
				log3A("[%d] execute append %s on key=%v, now value is %s", kv.me, op.Value, op.Key, v)
			} else if kvdb.Op_DELETE == commandType {
				delete(kv.tab, op.Key)
				log3A("[%d execute delete on key=%v", kv.me, op.Key)
			} else if GET != commandType {
				log.Printf("[%d] receive unknown request type,opType=%s", kv.me, op.OpType)
			}
			if ch, _ := kv.replyChan[commandIndex]; ch != nil {
				log3A("[%d] finish commandIndex=%d,commandTerm=%d", kv.me, commandIndex, commandTerm)
				ch <- ApplyResult{
					SessionId: sessionId,
					Term:      commandTerm,
				}
				close(ch)
				delete(kv.replyChan, commandIndex)
				log3A("[%d] close commandIndex=%d channel", kv.me, commandIndex)
			}
			if kv.maxRaftState > 0 && commandIndex == kv.nextSnapshotIndex {
				kv.nextSnapshotIndex = commandIndex + kv.maxRaftState
				snapshot, err := kv.makeSnapshot()
				if err != nil {
					err = &tool.RuntimeError{Stage: "make snapshot", Err: err}
					panic(err.Error())
				}
				kv.rf.Snapshot(commandIndex, snapshot)
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			err := kv.recoverFrom(msg.Snapshot)
			if err != nil {
				panic(err.Error())
			}
			if kv.commitIndex != msg.SnapshotIndex {
				log.Println("warning: commitIndex in snapshot is", kv.commitIndex, "but raft snapshot index is", msg.SnapshotIndex)
			}
			kv.mu.Unlock()
		} else {
			log.Println("warning:", kv.me, "receive unknown type log, log content:", msg)
		}
	}
}

// cleanupSessions scans the sessionMap to clean up the records corresponding to the expired session
func (kv *KVServer) cleanupSessions() {
	for {
		time.Sleep(SessionTimeout)
		kv.mu.Lock()
		var deleteArr []string

		for sessionId, lastVisitTime := range kv.sessionMap {
			if time.Since(lastVisitTime) >= SessionTimeout {
				deleteArr = append(deleteArr, sessionId)
			}
		}
		for _, s := range deleteArr {
			delete(kv.sessionMap, s)
		}
		kv.mu.Unlock()
	}
}
