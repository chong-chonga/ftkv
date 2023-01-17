package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/grpc"
	"kvraft/proto"
	"kvraft/raft"
	"kvraft/safegob"
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

const (
	OpenSession kvserver.Op = -2
	GET         kvserver.Op = -3
)

type Op struct {
	OpType kvserver.Op
	Key    string
	Value  string
	UUID   string
}

type ApplyResult struct {
	Term      int
	SessionId string
}

type KVServer struct {
	kvserver.kvserver
	mu            sync.Mutex // big lock
	me            int
	maxRaftState  int
	cleanupIndex  int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	persister     *tool.Storage
	replyChan     map[int]chan ApplyResult
	sessionMap    map[string]time.Time
	snapshotIndex int
	password      string

	// persistent
	commitIndex int
	uniqueId    int64
	tab         map[string]string
}

//
// StartKVServer starts a key/value server for rpc call which using raft to keep consistency
//
func StartKVServer(serverAddress []string, raftAddresses []string, me int, port int, storageFile *tool.Storage, maxRaftState int, confFile string) (*KVServer, error) {
	// call safegob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	safegob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftState = maxRaftState
	kv.persister = storageFile
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.replyChan = make(map[int]chan ApplyResult)
	kv.sessionMap = make(map[string]time.Time)
	snapshot := storageFile.ReadSnapshot()
	if nil != snapshot && len(snapshot) > 0 {
		kv.recoverFrom(snapshot)
	} else {
		kv.tab = make(map[string]string)
		kv.uniqueId = 0
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
	var err error
	kv.rf, err = raft.StartRaft(raftAddresses, me, port, storageFile, kv.applyCh)
	if err != nil {
		err = &raft.RuntimeError{Stage: "startraft", Err: err}
		return nil, err
	}
	go kv.startApply()
	go kv.cleanupSessions()

	// start grpc server
	listener, _ := net.Listen("tcp", serverAddress[me])
	server := grpc.NewServer()
	kvserver.RegisterKVServerServer(server, kv)
	go func() {
		_ = server.Serve(listener)
	}()
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

func (kv *KVServer) OpenSession(_ context.Context, request *kvserver.OpenSessionRequest) (*kvserver.OpenSessionReply, error) {
	reply := &kvserver.OpenSessionReply{}
	logSession("[%d] receive OpenSession request from client", kv.me)
	reply.SessionId = ""
	if request.GetPassword() != kv.password {
		reply.ErrCode = kvserver.ErrCode_INVALID_PASSWORD
		return reply, nil
	}
	command := Op{
		OpType: OpenSession,
		Key:    "",
		Value:  "",
		UUID:   uuid.NewV4().String(),
	}
	applyResult, errCode := kv.submit(command)
	if errCode == kvserver.ErrCode_OK {
		reply.SessionId = applyResult.SessionId
		reply.ErrCode = kvserver.ErrCode_OK
	} else {
		reply.ErrCode = errCode
	}
	return reply, nil
}

func (kv *KVServer) Get(_ context.Context, args *kvserver.GetRequest) (*kvserver.GetReply, error) {
	reply := &kvserver.GetReply{}
	log3A("[%d] receive get request from client, sessionId=%s", kv.me, args.SessionId)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.ErrCode = kvserver.ErrCode_WRONG_LEADER
		return reply, nil
	}
	if !kv.isSessionValid(args.SessionId) {
		reply.ErrCode = kvserver.ErrCode_INVALID_SESSION
		return reply, nil
	}
	command := Op{
		OpType: GET,
		Key:    args.Key,
		Value:  "",
	}
	log3A("[%d] start get request, sessionId=%s", kv.me, args.SessionId)
	// start时，不应当持有锁，因为这会阻塞 KVServer 接收来自 raft 的 log
	// start的命令的返回值的唯一性已经由 raft 的互斥锁保证
	_, errCode := kv.submit(command)
	if errCode == kvserver.ErrCode_OK {
		kv.mu.Lock()
		if v, exist := kv.tab[args.Key]; !exist {
			reply.ErrCode = kvserver.ErrCode_NO_KEY
			reply.Value = ""
		} else {
			reply.ErrCode = kvserver.ErrCode_OK
			reply.Value = v
		}
		kv.mu.Unlock()
	} else {
		reply.ErrCode = errCode
	}

	return reply, nil
}

func (kv *KVServer) Update(_ context.Context, args *kvserver.UpdateRequest) (*kvserver.UpdateReply, error) {
	reply := &kvserver.UpdateReply{}
	log3A("[%d] receive %v request from client, sessionId=%s", kv.me, args.Op, args.SessionId)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.ErrCode = kvserver.ErrCode_WRONG_LEADER
		return reply, nil
	}
	if !kv.isSessionValid(args.SessionId) {
		reply.ErrCode = kvserver.ErrCode_INVALID_SESSION
		return reply, nil
	}

	op := args.Op
	if op != kvserver.Op_PUT && op != kvserver.Op_APPEND && op != kvserver.Op_DELETE {
		reply.ErrCode = kvserver.ErrCode_INVALID_OP
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

func (kv *KVServer) isSessionValid(sessionId string) bool {
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
func (kv *KVServer) submit(op Op) (*ApplyResult, kvserver.ErrCode) {
	commandIndex, commandTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil, kvserver.ErrCode_WRONG_LEADER
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
		return &res, kvserver.ErrCode_OK
	} else {
		return nil, kvserver.ErrCode_WRONG_LEADER
	}
}

func (kv *KVServer) makeSnapshot() ([]byte, error) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	err := e.Encode(kv.uniqueId)
	if err != nil {
		return nil, errors.New("encode uniqueId fails: " + err.Error())
	}
	err = e.Encode(kv.tab)
	if err != nil {
		return nil, errors.New("encode tab fails: " + err.Error())
	}
	log3B("[%d] encode snapshot success!", kv.me)
	return w.Bytes(), nil
}

func (kv *KVServer) recoverFrom(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := safegob.NewDecoder(r)
	var tab map[string]string
	var nextClientId int64
	if d.Decode(&nextClientId) != nil ||
		d.Decode(&tab) != nil {
		log.Fatalf("[%d] decode snapshot failed!", kv.me)
	}
	kv.uniqueId = nextClientId
	kv.tab = tab
	log3B("[%d] read from snapshot success!", kv.me)
}

// startApply listen to the log sent from applyCh and execute the corresponding command.
// The same command will only be executed once
func (kv *KVServer) startApply() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			log3B("[%d] receive log, commandIndex=%d,commandTerm=%d", msg.CommandIndex, msg.CommandTerm)
			result := ApplyResult{
				Term: msg.CommandTerm,
			}
			kv.mu.Lock()
			if msg.CommandIndex < kv.commitIndex {
				kv.mu.Unlock()
				log.Println(kv.me, "ignore out dated log, expected log index", kv.commitIndex+1, "but receive", msg.CommandIndex)
				continue
			}
			op := msg.Command.(Op)
			commandType := op.OpType
			if OpenSession == commandType {
				result.SessionId = strconv.FormatInt(kv.uniqueId, 10) + "-" + op.UUID
				kv.sessionMap[result.SessionId] = time.Now()
				kv.uniqueId++
				logSession("[%d] open a new session, sessionId=%s", kv.me, result.SessionId)
			} else {
				if kvserver.Op_PUT == commandType {
					kv.tab[op.Key] = op.Value
					log3A("[%d] execute put %s on key=%v, sessionId=%s", kv.me, op.Value, op.Key, result.SessionId)
				} else if kvserver.Op_APPEND == commandType {
					v := kv.tab[op.Key]
					v += op.Value
					kv.tab[op.Key] = v
					log3A("[%d] execute append %s on key=%v, now value is %s, sessionId=%s", kv.me, op.Value, op.Key, v, result.SessionId)
				} else if kvserver.Op_DELETE == commandType {
					delete(kv.tab, op.Key)
					log3A("[%d execute delete on key=%v, sessionId=%s", kv.me, op.Key, result.SessionId)
				} else if GET != commandType {
					log.Printf("[%d] receive unknown request type,sessionId=%s,opType=%s", kv.me, result.SessionId, op.OpType)
				}
			}
			kv.commitIndex = msg.CommandIndex
			if ch, _ := kv.replyChan[kv.commitIndex]; ch != nil {
				log3A("[%d] finish commandIndex=%d,commandTerm=%d", kv.me, msg.CommandIndex, msg.CommandTerm)
				ch <- result
				close(ch)
				delete(kv.replyChan, kv.commitIndex)
				log3A("[%d] close commandIndex=%d channel", kv.me, msg.CommandIndex)
			}
			if kv.maxRaftState > 0 && kv.commitIndex == kv.snapshotIndex {
				kv.snapshotIndex += kv.maxRaftState
				snapshot, err := kv.makeSnapshot()
				if err != nil {
					err = &raft.RuntimeError{Stage: "makesnapshot", Err: err}
					panic(err.Error())
				}
				kv.rf.Snapshot(kv.commitIndex, snapshot)
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.recoverFrom(msg.Snapshot)
			kv.commitIndex = msg.SnapshotIndex
		} else {
			log.Println(kv.me, "receive unknown type log, content:", msg)
		}
	}
}

// cleanupSessions scans the sessionMap to clean up the records corresponding to the expired clientId
func (kv *KVServer) cleanupSessions() {
	for {
		time.Sleep(SessionTimeout)
		kv.mu.Lock()
		var deleteArr []string

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
