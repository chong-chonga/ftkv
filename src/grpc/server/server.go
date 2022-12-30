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
	"sync/atomic"
	"time"
)

const (
	Log3AEnabled      = false
	Log3BEnabled      = false
	LogSessionEnabled = true
)

const RequestTimeout = time.Duration(250) * time.Millisecond

const SessionTimeout = 1 * time.Hour

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrTimeout        = "ErrTimeout"
	ErrInvalidSession = "ErrInvalidSession"
)

const (
	OpenSession = "Open_Session"
	GET         = "Get"
	PUT         = "Put"
	APPEND      = "Append"
	DELETE      = "Delete"
)

type Op struct {
	OpType    string
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
	tempRequestId int64
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	persister     *storage.Storage
	replyChan     map[int]chan ApplyResult
	sessionMap    map[int64]time.Time

	// persistent
	commitIndex  int
	nextClientId int64
	requestMap   map[int64]int64
	tab          map[string]string
}

func log3A(format string, a ...interface{}) {
	// && (strings.Index(format, "lock") != -1 || strings.Index(format, "to ch") != -1)
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

func (kv *KVServer) generateTempRequestId() int64 {
	for {
		current := kv.tempRequestId
		if atomic.CompareAndSwapInt64(&kv.tempRequestId, current, current+1) {
			return current
		}
	}
}

func (kv *KVServer) OpenSession(_ context.Context, args *pb.OpenSessionArgs) (*pb.OpenSessionReply, error) {
	reply := &pb.OpenSessionReply{}
	logSession("[%d] receive OpenSession request from client", kv.me)
	reply.ClientId = -1
	tempRequestId := kv.generateTempRequestId()
	command := Op{
		OpType:    OpenSession,
		Key:       "",
		Value:     "",
		ClientId:  -1,
		RequestId: tempRequestId,
	}
	commandIndex, commandTerm, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return reply, nil
	}
	kv.mu.Lock()
	if c, _ := kv.replyChan[commandIndex]; c != nil {
		reply.Err = ErrTimeout
		kv.mu.Unlock()
		return reply, nil
	}
	ch := make(chan ApplyResult, 1)
	kv.replyChan[commandIndex] = ch
	kv.mu.Unlock()

	var res ApplyResult
	select {
	case res = <-ch:
		break
	case <-time.After(RequestTimeout):
		kv.mu.Lock()
		if c, _ := kv.replyChan[commandIndex]; c == nil {
			res = <-ch
			break
		}
		delete(kv.replyChan, commandIndex)
		kv.mu.Unlock()
		close(ch)
		reply.Err = ErrTimeout
		return reply, nil
	}

	if commandTerm == res.Term {
		kv.mu.Lock()
		kv.sessionMap[res.ClientId] = time.Now()
		kv.mu.Unlock()
		reply.ClientId = res.ClientId
		reply.Err = OK
	} else {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
	}
	return reply, nil
}

func (kv *KVServer) Get(_ context.Context, args *pb.GetArgs) (*pb.GetReply, error) {
	reply := &pb.GetReply{}
	log3A("[%d] receive get request from client, requestId=%d", kv.me, args.RequestId)
	kv.mu.Lock()
	if _, valid := kv.sessionMap[args.ClientId]; valid {
		kv.sessionMap[args.ClientId] = time.Now()
		kv.mu.Unlock()
	} else {
		kv.mu.Unlock()
		reply.Err = ErrInvalidSession
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
	commandIndex, commandTerm, isLeader := kv.rf.Start(command)

	log3A("[%d] started requestId=%d", kv.me, args.RequestId)
	if !isLeader {
		log3A("[%d] refuse get request from client because this server is not a leader! requestId=%d", kv.me, args.RequestId)
		reply.Err = ErrWrongLeader
		return reply, nil
	}
	log3A("[%d] submit get request, requestId=%d,commandIndex=%d", kv.me, args.RequestId, commandIndex)

	kv.mu.Lock()
	if c, _ := kv.replyChan[commandIndex]; c != nil {
		reply.Err = ErrTimeout
		kv.mu.Unlock()
		return reply, nil
	}
	ch := make(chan ApplyResult, 1)
	kv.replyChan[commandIndex] = ch
	kv.mu.Unlock()

	var res ApplyResult
	select {
	case res = <-ch:
		kv.mu.Lock()
		break
	case <-time.After(RequestTimeout):
		kv.mu.Lock()
		if c, _ := kv.replyChan[commandIndex]; c == nil {
			res = <-ch
			break
		}
		// clean up
		delete(kv.replyChan, commandIndex)
		kv.mu.Unlock()
		close(ch)
		reply.Err = ErrTimeout
		log3A("[%d] wait timeout, remove reply channel, requestId=%d", kv.me, args.RequestId)
		return reply, nil
	}

	defer kv.mu.Unlock()
	if res.Term == commandTerm {
		if v, exist := kv.tab[args.Key]; !exist {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value = v
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	return reply, nil
}

func (kv *KVServer) Update(c context.Context, args *pb.UpdateArgs) (*pb.UpdateReply, error) {
	reply := &pb.UpdateReply{}
	log3A("[%d] receive %v request from client, requestId=%d", kv.me, args.Op, args.RequestId)
	kv.mu.Lock()
	if _, valid := kv.sessionMap[args.ClientId]; valid {
		kv.sessionMap[args.ClientId] = time.Now()
		kv.mu.Unlock()
	} else {
		kv.mu.Unlock()
		reply.Err = ErrInvalidSession
		return reply, nil
	}
	command := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	log3A("[%d] start requestId=%d", kv.me, args.RequestId)
	commandIndex, commandTerm, isLeader := kv.rf.Start(command)

	log3A("[%d] started requestId=%d", kv.me, args.RequestId)
	if !isLeader {
		log3A("[%d] refuse %v request from client because this server is not a leader!, requestId=%d", kv.me, args.Op, args.RequestId)
		reply.Err = ErrWrongLeader
		return reply, nil
	}
	log3A("[%d] submit %v request, requestId=%d,commandIndex=%d", kv.me, args.Op, args.RequestId, commandIndex)

	kv.mu.Lock()
	if c, _ := kv.replyChan[commandIndex]; c != nil {
		reply.Err = ErrTimeout
		kv.mu.Unlock()
		return reply, nil
	}
	ch := make(chan ApplyResult, 1)
	kv.replyChan[commandIndex] = ch
	kv.mu.Unlock()

	var res ApplyResult
	select {
	case res = <-ch:
		kv.mu.Lock()
		break
	case <-time.After(RequestTimeout):
		kv.mu.Lock()
		if c, _ := kv.replyChan[commandIndex]; c == nil {
			res = <-ch
			break
		}
		delete(kv.replyChan, commandIndex)
		kv.mu.Unlock()
		close(ch)
		reply.Err = ErrTimeout
		log3A("[%d] wait timeout, remove reply channel, requestId=%d", kv.me, args.RequestId)
		return reply, nil
	}
	defer kv.mu.Unlock()

	if res.Term == commandTerm {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
	return reply, nil
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

func (kv *KVServer) readSnapshot(snapshot []byte) {
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
			if OpenSession == commandType {
				result.ClientId = kv.nextClientId
				kv.nextClientId++
				logSession("[%d] open a new session, clientId=%d", kv.me, result.ClientId)
			} else {
				if id, exists := kv.requestMap[op.ClientId]; !exists || requestId > id {
					if APPEND == commandType {
						v := kv.tab[op.Key]
						v += op.Value
						kv.tab[op.Key] = v
						log3A("[%d] execute append %s on key=%v, now value is %s, clientId=%d,requestId=%d", kv.me, op.Value, op.Key, v, op.ClientId, op.RequestId)
					} else if PUT == commandType {
						kv.tab[op.Key] = op.Value
						log3A("[%d] execute put %s on key=%v, clientId=%d,requestId=%d", kv.me, op.Value, op.Key, op.ClientId, op.RequestId)
					} else if DELETE == commandType {
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
			if kv.maxRaftState >= 0 && kv.persister.RaftStateSize() >= kv.maxRaftState {
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
					kv.readSnapshot(snapshot)
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
		for c := range kv.requestMap {
			lastVisitTime, visit := kv.sessionMap[c]
			if !visit || time.Since(lastVisitTime) >= SessionTimeout {
				deleteArr = append(deleteArr, c)
			}
		}
		for _, c := range deleteArr {
			delete(kv.sessionMap, c)
			delete(kv.requestMap, c)
		}
		kv.mu.Unlock()
	}
}

//
// StartKVServer starts a key/value server for rpc call which using raft to keep consistency
//
func StartKVServer(serverAddress []string, me int, storageFile *storage.Storage, maxraftstate int) *KVServer {
	// call safegob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	safegob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftState = maxraftstate
	kv.tempRequestId = 0
	kv.persister = storageFile
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.replyChan = make(map[int]chan ApplyResult)
	kv.sessionMap = make(map[int64]time.Time)
	snapshot := storageFile.ReadSnapshot()
	if nil != snapshot && len(snapshot) > 0 {
		kv.readSnapshot(snapshot)
	} else {
		kv.requestMap = make(map[int64]int64)
		kv.tab = make(map[string]string)
		kv.nextClientId = 0
		kv.commitIndex = 0
	}
	kv.rf = raft.StartRaft(serverAddress, me, storageFile, kv.applyCh)
	go kv.startApply()
	go kv.cleanupSessions()

	// start rpc server
	//server := rpc2.NewServer()
	//err := server.Register(kv)
	//if err != nil {
	//	log.Fatalln("start K/V server fail! errorInfo:", err)
	//}
	//err = server.Register(kv.rf)
	//if err != nil {
	//	log.Fatalln("start K/V server fail! errorInfo:", err)
	//}
	//mux := http.NewServeMux()
	//mux.Handle("/", server)
	//go http.ListenAndServe(serverAddress[me], server)

	// start grpc server
	net, _ := net.Listen("tcp", serverAddress[me])
	server := grpc.NewServer()
	pb.RegisterKVServerServer(server, kv)
	go server.Serve(net)
	return kv
}
