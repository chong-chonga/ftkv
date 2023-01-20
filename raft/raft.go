package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"kvraft/safegob"
	"kvraft/tool"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 为了方便使用日志进行debug或者进行测试, 使用常量开关来控制日志的打印
const (
	LogElectionEnabled    = false
	LogAppendEntryEnabled = false
	LogPersistRaftState   = false
	LogSnapshotEnabled    = false
)

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

func logElection(format string, v ...interface{}) {
	if LogElectionEnabled {
		log.Printf(format, v...)
	}
}

func logAppendEntry(format string, v ...interface{}) {
	if LogAppendEntryEnabled {
		log.Printf(format, v...)
	}
}

func logPersistence(format string, v ...interface{}) {
	if LogPersistRaftState {
		log.Printf(format, v...)
	}
}

func logSnapshot(format string, v ...interface{}) {
	if LogSnapshotEnabled {
		log.Printf(format, v...)
	}
}

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are committed,
// the peer send an ApplyMsg to the service on the same server, via the applyCh.
// CommandValid is true if and only if the ApplyMsg contains a newly committed log entry.
// similarly, SnapshotValid is true if and only if the ApplyMsg contains a snapshot
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// LogEntry describes an abstract command
type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu      sync.Mutex    // Lock to protect shared access to this peer's state
	peers   []*tool.Peer  // RPC clients of all peers
	storage *tool.Storage // tool for persistence
	me      int           // this peer's index into peers[]

	currentTerm       int        // persistent, the latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor          int        // persistent, candidateId that received vote in current term (or null if none)
	log               []LogEntry // persistent, log entries (first index is 1)
	lastIncludedIndex int        // persistent
	lastIncludedTerm  int        // persistent

	commitIndex      int    // index of the highest log entry known to be committed (initialized to 0)
	lastApplied      int    // index of the highest log entry known to be applied to state machine (initialized to 0)
	lastAppliedTerm  int    // term of lastApplied
	role             string // server role
	electionTimeout  int64  // time to start a new election, milliseconds
	minMajorityVotes int    // the minimum number of votes required to become a leader
	servers          int    // number of raft servers in cluster

	sendOrderCond *sync.Cond // condition for sendOrder
	sendOrder     int64      // which order can send log/snapshot to applyCh
	nextOrder     int64      // the next order a go routine will get

	applyCond *sync.Cond
	applyCh   chan ApplyMsg // for raft to send committed log and snapshot

	// for snapshot
	//snapshot []byte // in-memory snapshot

	// leader state
	nextIndex     []int   // index of the next log entry send to the other servers (initialized to 1 for each)
	matchIndex    []int   // index of the highest log entry known to be replicated on other servers
	heartbeatSent []int64 // time of last sent heartbeat for other servers (millisecond)
}

// StartRaft start raft server for service
// raftAddresses are other raft's ip address
// port specifies the raft server port
// me in the cluster shouldn't be duplicate but the order of raftAddresses can be random
func StartRaft(raftAddresses []string, me int, port int, storage *tool.Storage, applyCh chan ApplyMsg) (*Raft, error) {
	rf := &Raft{}
	rf.servers = len(raftAddresses) + 1
	if rf.servers&1 == 0 {
		return nil, &tool.RuntimeError{Stage: "start raft", Err: tool.ErrEvenServers}
	}
	if port <= 0 {
		return nil, &tool.RuntimeError{Stage: "start raft", Err: tool.ErrInvalidPort}
	}
	if storage == nil {
		return nil, &tool.RuntimeError{Stage: "start raft", Err: tool.ErrNilStorage}
	}

	// init rpc clients
	rf.peers = make([]*tool.Peer, len(raftAddresses))
	for i := 0; i < len(raftAddresses); i++ {
		rf.peers[i] = tool.MakePeer(raftAddresses[i])
	}

	rf.storage = storage
	rf.me = me
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.sendOrder = 0
	rf.nextOrder = 0
	rf.sendOrderCond = sync.NewCond(&sync.Mutex{})

	// initialize raft state according to Figure 2
	rf.role = Follower
	rf.minMajorityVotes = (rf.servers >> 1) + 1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = 0
	var logEntries = make([]LogEntry, 1)
	logEntries[0] = LogEntry{
		Term:    0,
		Command: nil,
	}
	rf.log = logEntries
	// every server has a default log entry in index 0
	// so index 0 is committed
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastAppliedTerm = 0
	if rf.minMajorityVotes < 2 {
		rf.commitIndex = rf.lastIncludedIndex + len(rf.log)
	}
	// initialize from persisted state
	err := rf.recoverFrom(storage.ReadRaftState())
	if err != nil {
		return nil, &tool.RuntimeError{Stage: "start raft", Err: err}
	}
	// start go rpc server
	server := rpc.NewServer()
	err = server.Register(rf)
	if err != nil {
		return nil, &tool.RuntimeError{Stage: "start raft", Err: err}
	}
	mux := http.NewServeMux()
	mux.Handle("/", server)
	go func() {
		_ = http.ListenAndServe(":"+strconv.Itoa(port), server)
	}()

	// now is safe, start go routines
	go rf.electionTicker()
	go rf.applyLog()

	log.Println("start raft success, raft port:", port)
	return rf, nil
}

// recoverFrom restore previously persisted state.
func (rf *Raft) recoverFrom(state []byte) error {
	if state == nil || len(state) < 1 {
		return nil
	}
	buf := bytes.NewBuffer(state)
	d := safegob.NewDecoder(buf)

	var currentTerm int
	var votedFor int
	var logEntries []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	var err error
	if err = d.Decode(&currentTerm); err != nil {
		return errors.New("recover from state: decode currentTerm fails: " + err.Error())
	}
	if err = d.Decode(&votedFor); err != nil {
		return errors.New("recover from state: decode votedFor fails: " + err.Error())
	}
	if err = d.Decode(&lastIncludedIndex); err != nil {
		return errors.New("recover from state: decode lastIncludedIndex fails: " + err.Error())
	}
	if err = d.Decode(&lastIncludedTerm); err != nil {
		return errors.New("recover from state: decode lastIncludedTerm fails: " + err.Error())
	}
	if err = d.Decode(&logEntries); err != nil {
		return errors.New("recover from state: decode logEntries fails: " + err.Error())
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = logEntries
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	if lastIncludedIndex > 0 {
		rf.lastApplied = lastIncludedIndex
		rf.lastAppliedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
	}
	logPersistence("[%d] restore raft state from persist success!", rf.me)
	return nil
}

// applyLog
// a go routine to send committed logs to applyCh
func (rf *Raft) applyLog() {
	for {
		rf.applyCond.L.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		startIndex := rf.lastApplied + 1
		endIndex := rf.commitIndex
		count := endIndex - startIndex + 1
		messages := make([]ApplyMsg, count)
		// if lastIncludedIndex is -1, then i = lastApplied + 1
		// otherwise, i = lastApplied + 1 - offset
		i := startIndex - rf.lastIncludedIndex - 1
		commandIndex := startIndex
		logTerm := -1
		for j := 0; j < count; j++ {
			logTerm = rf.log[i].Term
			messages[j] = ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: commandIndex,
				CommandTerm:  logTerm,
			}
			commandIndex++
			i++
		}
		rf.lastApplied = endIndex
		rf.lastAppliedTerm = logTerm
		order := rf.nextOrder
		rf.nextOrder++
		rf.applyCond.L.Unlock()

		rf.sendOrderCond.L.Lock()
		for rf.sendOrder != order {
			rf.sendOrderCond.Wait()
		}
		for _, message := range messages {
			rf.applyCh <- message
		}
		rf.sendOrder++
		rf.sendOrderCond.Broadcast()
		rf.sendOrderCond.L.Unlock()
	}
}

//
// Start is called when the service (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the Leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the Leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the Leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.role == Leader
	if isLeader {
		index = rf.lastIncludedIndex + 1 + len(rf.log)
		logAppendEntry("[%d] is leader, start agreement on log index %d", rf.me, index)
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		err := rf.persist()
		if err != nil {
			panic(err.Error())
		}
		if rf.minMajorityVotes < 2 {
			rf.commitIndex = index
		} else {
			for server := range rf.peers {
				rf.heartbeatSent[server] = currentMilli()
				go rf.sendHeartBeatMsg(server, term)
			}
		}
	}
	return index, term, isLeader
}

func (rf *Raft) makeRaftState() ([]byte, error) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		return nil, errors.New("encode currentTerm fails: " + err.Error())
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		return nil, errors.New("encode votedFor fails: " + err.Error())
	}
	err = e.Encode(rf.lastIncludedIndex)
	if err != nil {
		return nil, errors.New("encode lastIncludedIndex fails: " + err.Error())
	}
	err = e.Encode(rf.lastIncludedTerm)
	if err != nil {
		return nil, errors.New("encode lastIncludedTerm fails: " + err.Error())
	}
	err = e.Encode(rf.log)
	if err != nil {
		return nil, errors.New("encode log fails: " + err.Error())
	}
	return w.Bytes(), nil
}

//
// persist save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
func (rf *Raft) persist() error {
	state, err := rf.makeRaftState()
	if err != nil {
		return &tool.RuntimeError{Stage: "persist", Err: err}
	}
	err = rf.storage.SaveRaftState(state)
	if err != nil {
		return &tool.RuntimeError{Stage: "persist", Err: err}
	}
	logPersistence("[%d] persist raft state success!", rf.me)
	return nil
}

// GetState return currentTerm and whether this server believes it is the Leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term = rf.currentTerm
	var isLeader = strings.Compare(rf.role, Leader) == 0

	return term, isLeader
}

// Snapshot is called when the service has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	old := rf.lastIncludedIndex
	if old >= index {
		return
	}
	discardedLogIndex := index - old - 1
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[discardedLogIndex].Term
	var logs []LogEntry
	for i := discardedLogIndex + 1; i < len(rf.log); i++ {
		logs = append(logs, rf.log[i])
	}
	rf.log = logs
	state, err := rf.makeRaftState()
	if err != nil {
		e := &tool.RuntimeError{Stage: "snapshot", Err: err}
		panic(e.Error())
	}
	err = rf.storage.SaveStateAndSnapshot(state, snapshot)
	if err != nil {
		e := &tool.RuntimeError{Stage: "snapshot", Err: err}
		panic(e.Error())
	}
	logSnapshot("{%d] make a new snapshot, oldLastIncludedIndex=%d, newLastIncludedIndex=%d, trim log after %d", rf.me, old, index, discardedLogIndex)
}

// RPC arguments structure.

type RequestVoteArgs struct {
	Term         int // Candidate term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // index of Candidate's last log entry
	LastLogTerm  int // term of Candidate's last log entry
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Data              []byte // raw bytes of the snapshot
}

// RPC reply structure.

type RequestVoteReply struct {
	Term        int  // voter's current term
	VoteGranted bool // true when Candidate received vote
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contain entry matching prevLogIndex and prevLogTerm
	XTerm   int  // term of conflict log entry, -1 if follower don't have entry in PrevLogIndex
	XIndex  int  // index of first log entry in XTerm
	XLen    int  // length of log entry
}

type InstallSnapshotReply struct {
	Term int // current Term, for leader to update itself
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

// RequestVote
// RPC handler for RequestVote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	reply.VoteGranted = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return nil
	}

	termChanged := false
	defer func() {
		if termChanged || reply.VoteGranted {
			err := rf.persist()
			if err != nil {
				panic(err)
			}
		}
	}()
	// rules for all servers in Figure 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		termChanged = true
	}

	// RequestVote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate(args, rf) {
		logElection("[%d] vote for %d in term %d", rf.me, args.CandidateId, args.Term)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionTimeout = randomElectionTimeout()
	}

	return nil
}

// upToDate
// to determine whether candidate's log is more up-to-date
// (section 5.4.1 in Raft paper)
func upToDate(candidate *RequestVoteArgs, voter *Raft) bool {
	logLen := len(voter.log)
	voterLastLogIndex := voter.lastIncludedIndex + logLen

	voterLastLogTerm := voter.lastIncludedTerm
	if logLen > 0 {
		voterLastLogTerm = voter.log[logLen-1].Term
	}
	candidateLastLogIndex := candidate.LastLogIndex
	candidateLastLogTerm := candidate.LastLogTerm

	return candidateLastLogTerm > voterLastLogTerm || (candidateLastLogTerm == voterLastLogTerm && candidateLastLogIndex >= voterLastLogIndex)
}

// AppendEntries
// RPC handler for AppendEntries
// 超时时间的重置是有条件的，只有当这个 leader 的 term 不小于当前的 follower 时，才会重置定时器(承认leader的有效性)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	reply.Success = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	totalLogLen := rf.lastIncludedIndex + len(rf.log) + 1
	reply.Term = rf.currentTerm
	reply.XLen = totalLogLen
	logElection("[%d] receive append rpc from %d in term %d", rf.me, args.LeaderId, args.Term)

	// reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return nil
	}
	termChanged := false
	truncated := false
	appended := false
	defer func() {
		if termChanged || truncated || appended {
			err := rf.persist()
			if err != nil {
				panic(err.Error())
			}
		}
	}()
	// rules for all servers in Figure 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		termChanged = true
	}

	// if prevLogIndex points beyond the end of log
	// set XTerm to -1, so nextIndex can set to XLen
	// reply false
	if args.PrevLogIndex >= totalLogLen {
		reply.XTerm = -1
		reply.XIndex = -1
		rf.electionTimeout = randomElectionTimeout()
		return nil
	}

	// 对于leader发送的 prevLogIndex 和follower的 lastIncludedIndex 有三种情况
	// 1. PrevLogIndex > lastIncludedIndex
	// 2. PrevLogIndex < lastIncludedIndex（收到AppendEntries RPC 可能是过时的）
	// 3. PrevLogIndex == lastIncludedIndex（收到AppendEntries RPC 可能是过时的）
	// 第二种、三种情况，我们无需比较，因为Snapshot中的log都是committed，
	// 根据Figure3的特性可知, leader 肯定包含有最新的 committed log，因此 leader 和 follower 至少会在 lastIncludedIndex 上有相同的 log

	idx := 0
	i := 0
	//prevLogIndex := args.PrevLogIndex - rf.lastIncludedIndex - 1
	offset := args.PrevLogIndex - rf.lastIncludedIndex
	if offset > 0 {
		/// offset > 0：需要比较第 offset 个 log 的 term，这里减1是为了弥补数组索引，lastIncludedIndex 初始化为 -1 也是如此
		offset -= 1
		// if term of log entry in prevLogIndex not match prevLogTerm
		// set XTerm to term of the log
		// set XIndex to the first entry in XTerm
		// reply false (§5.3)
		if rf.log[offset].Term != args.PrevLogTerm {
			reply.XTerm = rf.log[offset].Term
			for offset > 0 {
				if rf.log[offset-1].Term != reply.XTerm {
					break
				}
				offset--
			}
			reply.XIndex = offset + rf.lastIncludedIndex + 1
			rf.electionTimeout = randomElectionTimeout()
			return nil
		}
		// match, set i to prevLogIndex + 1, prepare for comparing the following logs
		i = offset + 1
	} else {
		// offset <= 0：说明log在snapshot中，则令idx加上偏移量，比较idx及其之后的log
		idx -= offset
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	for ; i < len(rf.log) && idx < len(args.Entries); i++ {
		if rf.log[i].Term == args.Entries[idx].Term {
			idx++
			continue
		}
		var logs []LogEntry
		for j := 0; j < i; j++ {
			logs = append(logs, rf.log[j])
		}
		rf.log = logs
		logAppendEntry("[%d] exists conflict entry in index %d, delete the entry and all after that", rf.me, i)
		truncated = true
		break
	}
	appended = idx < len(args.Entries)
	// append any new entries not already in the log
	if idx < len(args.Entries) {
		count := len(args.Entries) - idx
		for idx < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[idx])
			idx++
		}
		logAppendEntry("[%d] append %d new entries", rf.me, count)
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		maxLogIndex := rf.lastIncludedIndex + len(rf.log)
		if args.LeaderCommit < maxLogIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = maxLogIndex
		}
		logAppendEntry("[%d] update commitIndex to %d", rf.me, rf.commitIndex)
		rf.applyCond.Signal()
	}
	reply.Success = true
	rf.electionTimeout = randomElectionTimeout()
	return nil
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	// reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return nil
	}

	termChanged := false
	defer func() {
		if termChanged {
			err := rf.persist()
			if err != nil {
				panic(err.Error())
			}
		}
	}()
	// rules for all servers in Figure 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		termChanged = true
	}

	lastIncludedIndex := args.LastIncludedIndex
	lastIncludedTerm := args.LastIncludedTerm
	if rf.lastIncludedIndex >= lastIncludedIndex || rf.lastIncludedTerm > lastIncludedTerm ||
		rf.lastApplied > lastIncludedIndex || rf.lastAppliedTerm > lastIncludedTerm {
		rf.mu.Unlock()
		return nil
	}

	snapshot := args.Data
	old := rf.lastIncludedIndex
	reservedIndex := lastIncludedIndex - old
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastApplied = lastIncludedIndex
	var logs []LogEntry
	for i := reservedIndex; i < len(rf.log); i++ {
		logs = append(logs, rf.log[i])
	}
	rf.log = logs
	state, err := rf.makeRaftState()
	if err != nil {
		e := &tool.RuntimeError{Stage: "condinstallsnapshot", Err: err}
		panic(e.Error())
	}
	err = rf.storage.SaveStateAndSnapshot(state, snapshot)
	if err != nil {
		e := &tool.RuntimeError{Stage: "condinstallsnapshot", Err: err}
		panic(e.Error())
	}
	message := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	logSnapshot("[%d] receive snapshot from %d, lastIncludedIndex=%d, lastIncludedTerm=%d", rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.electionTimeout = randomElectionTimeout()
	order := rf.nextOrder
	rf.nextOrder++
	rf.mu.Unlock()

	go func(order int64) {
		rf.sendOrderCond.L.Lock()
		for rf.sendOrder != order {
			rf.sendOrderCond.Wait()
		}
		rf.applyCh <- message
		rf.sendOrder++
		rf.sendOrderCond.Broadcast()
		rf.sendOrderCond.L.Unlock()
	}(order)
	return nil
}

func randomElectionTimeout() int64 {
	timeout := randTimeout()
	now := currentMilli()
	return timeout + now
}

func randTimeout() int64 {
	// 100ms per heartbeat, election timeout can be at least 200ms
	return rand.Int63n(150) + 200
}

func currentMilli() int64 {
	return time.Now().UnixNano() / 1000000
}

// The ticker go routine starts a new election if this peer hasn't received heartbeat recently.
func (rf *Raft) electionTicker() {
	time.Sleep(1 * time.Second)
	rf.electionTimeout = randomElectionTimeout()
	for {
		rf.mu.Lock()
		electionTimeout := rf.electionTimeout
		if currentMilli() >= electionTimeout {
			if rf.role != Leader {
				go rf.attemptElection()
			}
			electionTimeout = randomElectionTimeout()
			rf.electionTimeout = electionTimeout
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(electionTimeout-currentMilli()) * time.Millisecond)
	}
}

func (rf *Raft) attemptElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	err := rf.persist()
	if err != nil {
		panic(err.Error())
	}
	if rf.minMajorityVotes < 2 {
		rf.role = Leader
		rf.mu.Unlock()
		return
	}
	rf.role = Candidate
	votes := 1

	logCount := len(rf.log)
	var lastLogTerm = rf.lastIncludedTerm
	if logCount > 0 {
		lastLogTerm = rf.log[logCount-1].Term
	}
	lastLogIndex := rf.lastIncludedIndex + logCount

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	done := false
	for server := range rf.peers {
		go func(server int) {
			reply := RequestVoteReply{}
			logElection("[%d] request vote from %d in term %d", rf.me, server, args.Term)
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// rules for all servers in Figure 2
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
					err = rf.persist()
					if err != nil {
						panic(err.Error())
					}
					return
				}
				// term 或 role 发生改变时，当前 election 作废
				if done || rf.role != Candidate || rf.currentTerm != args.Term {
					return
				}
				if reply.VoteGranted {
					votes++
					logElection("[%d] got vote from %d in term %d", rf.me, server, args.Term)
					if votes >= rf.minMajorityVotes {
						logElection("[%d] got majority votes in term %d, [%d] is leader!", rf.me, args.Term, rf.me)
						done = true
						otherServers := rf.servers - 1
						rf.role = Leader
						rf.nextIndex = make([]int, otherServers)
						rf.matchIndex = make([]int, otherServers)
						rf.heartbeatSent = make([]int64, otherServers)
						totalLogCount := rf.lastIncludedIndex + len(rf.log) + 1
						for i := 0; i < otherServers; i++ {
							rf.nextIndex[i] = totalLogCount
							rf.matchIndex[i] = 0
						}
						for i := 0; i < otherServers; i++ {
							rf.heartbeatSent[i] = currentMilli() - 200
							go rf.heartbeat(i, rf.currentTerm)
						}
					}
				}
			}
		}(server)
	}

}

func (rf *Raft) heartbeat(server int, term int) {
	for true {
		for {
			rf.mu.Lock()
			if rf.role != Leader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			lastSentTime := rf.heartbeatSent[server]
			past := currentMilli() - lastSentTime
			if past >= 100 {
				rf.heartbeatSent[server] = currentMilli()
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			time.Sleep(time.Duration(100-past) * time.Millisecond)
		}
		rf.sendHeartBeatMsg(server, term)
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeatMsg(server int, term int) {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[server]
	logCount := len(rf.log)
	lastLogIndex := rf.lastIncludedIndex + logCount
	var entries []LogEntry
	if nextIndex <= lastLogIndex {
		startIndex := nextIndex - rf.lastIncludedIndex - 1
		if startIndex < 0 {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.storage.ReadSnapshot(),
			}
			matchIndex := rf.matchIndex[server]
			rf.mu.Unlock()
			go func(args InstallSnapshotArgs, nextIndex int, matchIndex int) {
				logSnapshot("[%d] send snapshot to %d, last included index is %d", args.LeaderId, server, args.LastIncludedIndex)
				reply := InstallSnapshotReply{}
				ok := rf.sendInstallSnapshot(server, &args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.role = Follower
						rf.votedFor = -1
						rf.currentTerm = reply.Term
						rf.electionTimeout = randomElectionTimeout()
						err := rf.persist()
						if err != nil {
							panic(err.Error())
						}
						return
					}
					if rf.role != Leader || rf.currentTerm != term {
						return
					}
					// if an outdated reply is received, ignore it
					if rf.nextIndex[server] > nextIndex || rf.matchIndex[server] > matchIndex {
						return
					}
					// logs in snapshot are committed, no need to update commitIndex
					rf.matchIndex[server] = args.LastIncludedIndex
					rf.nextIndex[server] = args.LastIncludedIndex + 1
				}
			}(args, nextIndex, matchIndex)
			return
		}
		for startIndex < logCount {
			entries = append(entries, rf.log[startIndex])
			startIndex++
		}
	}

	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.lastIncludedTerm
	if prevLogIndex > rf.lastIncludedIndex {
		prevLogTerm = rf.log[prevLogIndex-rf.lastIncludedIndex-1].Term
	}

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
	// If successful: update nextIndex and matchIndex for follower (§5.3)
	// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	go func(args AppendEntriesArgs, lastIndex int) {
		logElection("[%d] send append rpc to %d in term %d", args.LeaderId, server, args.Term)
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.role = Follower
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.electionTimeout = randomElectionTimeout()
				err := rf.persist()
				if err != nil {
					panic(err.Error())
				}
				return
			}
			if rf.role != Leader || rf.currentTerm != term {
				return
			}
			if reply.Success {
				// if an outdated reply is received, ignore it
				if rf.nextIndex[server] > lastIndex {
					return
				}
				// update nextIndex and matchIndex， then update commitIndex
				rf.nextIndex[server] = lastIndex + 1
				rf.matchIndex[server] = lastIndex
				logAppendEntry("[%d] receive success append reply from %d, last append index is %d", rf.me, server, lastIndex)
				index := majorityCommitIndex(rf)
				if rf.commitIndex < index {
					rf.commitIndex = index
					logAppendEntry("[%d] has replicated log in index %d on majority servers, update commitIndex to %d", rf.me, index, index)
					rf.applyCond.Signal()
				}
			} else {
				rollbackIndex := backupIndex(reply, rf.log, args.PrevLogIndex, rf.lastIncludedIndex)
				if rollbackIndex <= rf.nextIndex[server] {
					rf.nextIndex[server] = rollbackIndex
					logAppendEntry("[%d] append to %d failed, nextIndex back up to %d", rf.me, server, rollbackIndex)
				}
			}
		}
	}(args, lastLogIndex)
}

// majorityCommitIndex
// find the Nth log, which a majority of matchIndex[i] >= N and its term is leader's term
func majorityCommitIndex(rf *Raft) int {
	servers := rf.servers
	arr := make([]int, servers)
	i := 0

	for i < servers-1 {
		arr[i] = rf.matchIndex[i]
		i++
	}
	arr[i] = rf.lastIncludedIndex + len(rf.log)
	sort.Ints(arr)
	index := rf.commitIndex
	midCommit := arr[servers>>1]
	// according to Figure 8
	// the preceding logs may be overwritten by other leaders
	// only if leader replicates an entry from its current term on a majority of the servers before crashing
	// then all preceding logs are committed
	for i = midCommit; i >= rf.commitIndex+1; i-- {
		if rf.log[i-rf.lastIncludedIndex-1].Term == rf.currentTerm {
			return i
		}
	}
	return index
}

// backupIndex
// is called by leader when AppendEntries fails
// to determine the nextIndex for the corresponding follower
// which is mentioned in https://www.youtube.com/watch?v=4r8Mz3MMivY Backup Faster section
func backupIndex(reply AppendEntriesReply, log []LogEntry, prevLogIndex int, lastIncludedIndex int) int {
	if reply.XTerm == -1 {
		// follower don't have log entry in prevLogIndex, set nextIndex to XLen
		return reply.XLen
	} else {
		for i := prevLogIndex - lastIncludedIndex - 2; i >= 0; i-- {
			logTerm := log[i].Term
			if logTerm == reply.XTerm {
				// leader have log entry in XTerm
				// set nextIndex to the index of last log entry in leader in XTerm
				return i + lastIncludedIndex + 1
			} else if logTerm < reply.XTerm {
				break
			}
		}
	}
	// leader don't have log entry in XTerm
	// set nextIndex to the index of first log entry in follower in XTerm
	return reply.XIndex
}
