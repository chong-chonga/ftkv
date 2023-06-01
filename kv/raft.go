// this Raft is specified for shardkv server

package kv

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/ftkv/v1/raft"
	"github.com/ftkv/v1/storage"
	"github.com/ftkv/v1/tool"
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

const heartbeatInterval = 100 // millisecond

const defaultRandomInterval = 150

const defaultElectionTimeout = 350

const (
	follower  = "follower"
	candidate = "candidate"
	leader    = "leader"
)

// applyMsg
// as each Raft peer becomes aware that successive log entries are committed,
// the peer send an applyMsg to the service on the same server, via the applyCh.
// CommandValid is true if and only if the applyMsg contains a newly committed log entry.
// similarly, SnapshotValid is true if and only if the applyMsg contains a snapshot
//
type applyMsg struct {
	CommandValid bool
	Command      command
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
	Command command
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	// initialize when starting
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	applyCond *sync.Cond
	storage   *storage.RaftStorage // tool for persistence

	applyCh       chan applyMsg // for Raft to send committed log and snapshot
	sendOrderCond *sync.Cond    // condition for sendOrder
	sendOrder     int64         // which order can send log/snapshot to applyCh
	nextOrder     int64         // the next order a go routine will get

	state           string // server role
	commitIndex     int    // index of the highest log entry known to be committed (initialized to 0)
	lastApplied     int    // index of the highest log entry known to be applied to state machine (initialized to 0)
	lastAppliedTerm int    // term of lastApplied
	timeToElection  int64  // time to start a new election, milliseconds

	// initialize when Raft becomes leader
	nextIndex     []int   // index of the next log entry send to the other servers (initialized to 1 for each)
	matchIndex    []int   // index of the highest log entry known to be replicated on other servers
	heartbeatSent []int64 // time of last sent heartbeat for other servers (millisecond)

	// persistent
	currentTerm       int        // the latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor          int        // candidateId that received vote in current term (or null if none)
	log               []LogEntry // log entries (first index is 1)
	lastIncludedIndex int        // the snapshot replaces all entries up through and including this index
	lastIncludedTerm  int        // term of lastIncludedIndex

	// configurable
	me                        int          // used for candidateId in RPC
	peers                     []*tool.Peer // RPC clients of other peers
	randomInterval            int
	minElectionTimeout        int
	requestVoteLogEnabled     bool
	appendEntryLogEnabled     bool
	installSnapshotLogEnabled bool
	persistLogEnabled         bool
}

func currentMilli() int64 {
	return time.Now().UnixNano() / 1000000
}

// startRaft start Raft server for service
// raftAddresses are other Raft's ip address
// port specifies the Raft server port
// me in the cluster shouldn't be duplicate but the order of raftAddresses can be random
func startRaft(conf raft.Config, storage *storage.RaftStorage, applyCh chan applyMsg) (*Raft, error) {

	rf := &Raft{}
	port := conf.Port
	if port <= 0 {
		return nil, &tool.RuntimeError{Stage: "configure Raft", Err: errors.New("Raft server port " + strconv.Itoa(port) + " is invalid")}
	}
	if storage == nil {
		return nil, &tool.RuntimeError{Stage: "configure Raft", Err: errors.New("storage is nil")}
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.storage = storage
	rf.applyCh = applyCh
	rf.sendOrderCond = sync.NewCond(&sync.Mutex{})
	rf.sendOrder = 0
	rf.nextOrder = 0

	// initialize Raft state according to Figure 2
	rf.state = follower
	// every server has a default log entry in index 0
	// so index 0 is committed
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastAppliedTerm = 0

	// restore from persisted state
	var err error
	state := storage.GetRaftState()
	if state != nil && len(state) > 0 {
		err = rf.recoverFrom(state)
		if err != nil {
			return nil, &tool.RuntimeError{Stage: "restore Raft state", Err: err}
		}
	} else {
		rf.currentTerm = 0
		rf.votedFor = -1
		var logEntries = make([]LogEntry, 1)
		logEntries[0] = LogEntry{
			Term: 0,
		}
		rf.log = logEntries
		rf.lastIncludedIndex = -1
		rf.lastIncludedTerm = 0
	}

	// apply configuration
	addresses := conf.ServerAddresses
	if len(addresses) > 0 {
		err = tool.Check(addresses)
		if err != nil {
			return nil, &tool.RuntimeError{Stage: "configure Raft", Err: err}
		}
	}
	totalServers := len(addresses) + 1
	peers := make([]*tool.Peer, totalServers-1)
	for i := 0; i < len(addresses); i++ {
		peers[i] = tool.MakePeer(addresses[i])
	}
	rf.me = conf.RaftId
	rf.peers = peers
	majorityVotes := (totalServers >> 1) + 1
	if majorityVotes < 2 {
		rf.commitIndex = rf.lastIncludedIndex + len(rf.log)
	}
	log.Printf("configure Raft info: total Raft servers is %d, majority number is %d, address of other rafts: %v", totalServers, majorityVotes, addresses)
	randomInterval := conf.RandomInterval
	if randomInterval < 0 {
		return nil, &tool.RuntimeError{Stage: "configure Raft", Err: errors.New("invalid random interval")}
	} else if randomInterval == 0 {
		randomInterval = defaultRandomInterval
	}
	electionTimeout := conf.MinElectionTimeout
	if electionTimeout < 0 {
		return nil, &tool.RuntimeError{Stage: "configure Raft", Err: errors.New("invalid election timeout")}
	} else if electionTimeout == 0 {
		electionTimeout = defaultElectionTimeout
	}
	rf.randomInterval = conf.RandomInterval
	rf.minElectionTimeout = conf.MinElectionTimeout
	log.Printf("configure Raft info: minimum election timeout is %dms, maximum election timeout is %dms", conf.MinElectionTimeout, conf.MinElectionTimeout+conf.RandomInterval)
	if conf.Log.RequestVoteEnabled {
		rf.requestVoteLogEnabled = true
		log.Println("configure Raft info: enable request vote log")
	}
	if conf.Log.AppendEntryEnabled {
		rf.appendEntryLogEnabled = true
		log.Println("configure Raft info: enable append entry log")
	}
	if conf.Log.InstallSnapshotEnabled {
		rf.installSnapshotLogEnabled = true
		log.Println("configure Raft info: enable install snapshot log")
	}
	if conf.Log.PersistEnabled {
		rf.persistLogEnabled = true
		log.Println("configure Raft info: enable persist log")
	}

	// start go rpc server
	server := rpc.NewServer()
	err = server.Register(rf)
	if err != nil {
		return nil, &tool.RuntimeError{Stage: "start Raft", Err: err}
	}
	mux := http.NewServeMux()
	mux.Handle("/", server)
	go func() {
		_ = http.ListenAndServe(":"+strconv.Itoa(port), server)
	}()

	// now is safe, start go routines
	go rf.electionTicker()
	go rf.applyLog()

	log.Println("start Raft server success, serves at port", port, ", other Raft server addresses:", addresses)
	return rf, nil
}

func (rf *Raft) logRequestVote(format string, v ...interface{}) {
	if rf.requestVoteLogEnabled {
		log.Printf(format, v...)
	}
}

func (rf *Raft) logAppendEntry(format string, v ...interface{}) {
	if rf.appendEntryLogEnabled {
		log.Printf(format, v...)
	}
}

func (rf *Raft) logInstallSnapshot(format string, v ...interface{}) {
	if rf.installSnapshotLogEnabled {
		log.Printf(format, v...)
	}
}

func (rf *Raft) logPersistence(format string, v ...interface{}) {
	if rf.persistLogEnabled {
		log.Printf(format, v...)
	}
}

func (rf *Raft) makeRaftState() ([]byte, error) {
	w := new(bytes.Buffer)
	e := json.NewEncoder(w)
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

// recoverFrom restore previously persisted state.
func (rf *Raft) recoverFrom(state []byte) error {
	if state == nil || len(state) < 1 {
		return nil
	}
	buf := bytes.NewBuffer(state)
	d := json.NewDecoder(buf)
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
	rf.logPersistence("[%d] restore Raft state from persist success!", rf.me)
	return nil
}

func (rf *Raft) sendApplyMessages(order int64, messages []applyMsg) {
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
		messages := make([]applyMsg, count)
		// if lastIncludedIndex is -1, then i = lastApplied + 1
		// otherwise, i = lastApplied + 1 - offset
		i := startIndex - rf.lastIncludedIndex - 1
		commandIndex := startIndex
		logTerm := -1
		for j := 0; j < count; j++ {
			logTerm = rf.log[i].Term
			messages[j] = applyMsg{
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

		rf.sendApplyMessages(order, messages)
	}
}

//
// Start is called when the service (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command command) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == leader
	if isLeader {
		index = rf.lastIncludedIndex + 1 + len(rf.log)
		rf.logAppendEntry("[%d] is leader, start agreement on log index %d", rf.me, index)
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		err := rf.persist()
		if err != nil {
			panic(err.Error())
		}
		peers := rf.peers
		if len(peers) == 0 {
			rf.commitIndex = index
			rf.applyCond.Signal()
		} else {
			for server := range peers {
				rf.heartbeatSent[server] = currentMilli()
				go rf.sendHeartbeatMessage(server, term)
			}
		}
	}
	return index, term, isLeader
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
	rf.logPersistence("[%d] save Raft state success!", rf.me)
	return nil
}

// GetState return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	var term = rf.currentTerm
	var isLeader = strings.Compare(rf.state, leader) == 0

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

	rf.logInstallSnapshot("{%d] make a new snapshot, oldLastIncludedIndex=%d, newLastIncludedIndex=%d, trim log after %d", rf.me, old, index, discardedLogIndex)
}

// RPC arguments structure.

type RequestVoteArgs struct {
	Term         int // candidate term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
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
	VoteGranted bool // true when candidate received vote
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

func (rf *Raft) resetTimeout() int64 {
	timeout := rand.Intn(rf.randomInterval) + rf.minElectionTimeout
	now := currentMilli()
	timeToElection := int64(timeout) + now
	rf.timeToElection = timeToElection
	return timeToElection
}

func (rf *Raft) toFollower(term int) {
	rf.currentTerm = term
	rf.state = follower
	rf.votedFor = -1
}

// sendVoteRequestsAndHandleReply send RequestVote RPC to other servers for election
// args is pointer of RequestVote RPC arguments
// peers is rpc clients
// majorityVotes is the minimum number of votes required to become a leader
func (rf *Raft) sendVoteRequestsAndHandleReply(args *RequestVoteArgs, peers []*tool.Peer, majorityVotes int) {
	votes := 1
	for _, peer := range peers {
		go func(server *tool.Peer) {
			reply := &RequestVoteReply{}
			rf.logRequestVote("[%d] request vote from %s in term %d", rf.me, server.GetAddr(), args.Term)
			err := server.Call("Raft.RequestVote", args, reply)
			if err != nil {
				rf.logRequestVote("send request vote fail, err=%v", err)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// rules for all servers in Figure 2
			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term)
				err := rf.persist()
				if err != nil {
					panic(err.Error())
				}
				rf.resetTimeout()
				rf.logRequestVote("[%d] change to follower, because %s term is bigger than currentTerm", rf.me, server.GetAddr())
				return
			}
			// term 或 role 发生改变时，当前 election 作废
			if rf.state != candidate || rf.currentTerm != args.Term {
				rf.logRequestVote("[%d] abandon the current election in term %d", rf.me, args.Term)
				return
			}
			if reply.VoteGranted {
				votes++
				rf.logRequestVote("[%d] got vote from %s in term %d", rf.me, server.GetAddr(), args.Term)
				if votes >= majorityVotes {
					rf.logRequestVote("[%d] got majority votes, is leader in term %d", rf.me, args.Term)
					followers := len(rf.peers)
					rf.state = leader
					rf.nextIndex = make([]int, followers)
					rf.matchIndex = make([]int, followers)
					rf.heartbeatSent = make([]int64, followers)
					totalLogCount := rf.lastIncludedIndex + len(rf.log) + 1
					for i := 0; i < followers; i++ {
						rf.nextIndex[i] = totalLogCount
						rf.matchIndex[i] = 0
					}
					for i := 0; i < followers; i++ {
						rf.heartbeatSent[i] = 0
						go rf.heartbeat(i, rf.currentTerm)
					}
				}
			}
		}(peer)
	}

}

func (rf *Raft) sendSnapshotAndHandleReply(server int, term int, args *InstallSnapshotArgs) {
	peer := rf.peers[server]
	rf.logInstallSnapshot("[%d] send snapshot to %s, lastIncludedIndex=%d", args.LeaderId, peer.GetAddr(), args.LastIncludedIndex)
	reply := &InstallSnapshotReply{}
	err := peer.Call("Raft.InstallSnapshot", args, reply)
	if err != nil {
		rf.logInstallSnapshot("send snapshot fail, err=%v", err)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		err := rf.persist()
		if err != nil {
			panic(err.Error())
		}
		rf.resetTimeout()
		return
	}
	if rf.state != leader || rf.currentTerm != term {
		return
	}
	// if an outdated reply is received, ignore it
	if rf.nextIndex[server] > args.LastIncludedIndex || rf.matchIndex[server] >= args.LastIncludedIndex {
		return
	}
	// logs in snapshot are committed, no need to update commitIndex
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.logInstallSnapshot("[%d] receive InstallSnapshot success reply from %s, lastIncludedIndex=%d", args.LeaderId, peer.GetAddr(), args.LastIncludedIndex)
}

// If successful: update nextIndex and matchIndex for follower (§5.3)
// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (rf *Raft) sendEntriesAndHandleReply(server int, lastIndex int, term int, args *AppendEntriesArgs) {
	peer := rf.peers[server]
	rf.logAppendEntry("[%d] send append rpc to %d in term %d", args.LeaderId, server, args.Term)
	reply := &AppendEntriesReply{}
	err := peer.Call("Raft.AppendEntries", args, reply)
	if err != nil {
		rf.logAppendEntry("send entries fail, err=%v", err)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		err := rf.persist()
		if err != nil {
			panic(err.Error())
		}
		rf.resetTimeout()
		return
	}
	if rf.state != leader || rf.currentTerm != term {
		return
	}
	if reply.Success {
		rf.logAppendEntry("[%d] receive success append reply from %s, last append index is %d", rf.me, peer.GetAddr(), lastIndex)
		if rf.nextIndex[server] > lastIndex {
			return
		}
		rf.nextIndex[server] = lastIndex + 1
		rf.matchIndex[server] = lastIndex
		index := majorityCommitIndex(rf)
		if rf.commitIndex < index {
			rf.commitIndex = index
			rf.logAppendEntry("[%d] has replicated log on majority servers, update commitIndex to %d", rf.me, index)
			rf.applyCond.Signal()
		}
	} else {
		rollbackIndex := backupIndex(reply, rf.log, args.PrevLogIndex, rf.lastIncludedIndex)
		if rollbackIndex <= rf.nextIndex[server] {
			rf.nextIndex[server] = rollbackIndex
			rf.logAppendEntry("[%d] append to %s failed, nextIndex back up to %d", rf.me, peer.GetAddr(), rollbackIndex)
		}
	}
}

// RequestVote
// RPC handler for RequestVote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.logRequestVote("[%d] receive vote request from %d", rf.me, args.CandidateId)
	reply.VoteGranted = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		rf.logRequestVote("[%d] currentTerm is bigger than %d, refuse vote", rf.me, args.CandidateId)
		return nil
	}

	termChanged := false
	vote := false
	defer func() {
		if termChanged || vote {
			err := rf.persist()
			if err != nil {
				panic(err)
			}
			// If election timeout elapses without receiving AppendEntries
			// RPC from current leader or granting vote to candidate:
			// convert to candidate
			rf.resetTimeout()
		}
	}()

	// rules for all servers in Figure 2
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
		termChanged = true
	}

	// RequestVote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate(args, rf) {
		rf.logRequestVote("[%d] vote for %d in term %d", rf.me, args.CandidateId, args.Term)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		vote = true
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
	rf.logAppendEntry("[%d] receive append rpc from %d in term %d", rf.me, args.LeaderId, args.Term)
	reply.Success = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	totalLogLen := rf.lastIncludedIndex + len(rf.log) + 1
	reply.Term = rf.currentTerm
	reply.XLen = totalLogLen

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
		rf.resetTimeout()
	}()
	// rules for all servers in Figure 2
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
		termChanged = true
	}

	// if prevLogIndex points beyond the end of log
	// set XTerm to -1, so nextIndex can set to XLen
	// reply false
	if args.PrevLogIndex >= totalLogLen {
		reply.XTerm = -1
		reply.XIndex = -1
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
		rf.logAppendEntry("[%d] exists conflict entry in index %d, delete the entry and all after that", rf.me, i)
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
		rf.logAppendEntry("[%d] append %d new entries", rf.me, count)
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		maxLogIndex := rf.lastIncludedIndex + len(rf.log)
		if args.LeaderCommit < maxLogIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = maxLogIndex
		}
		rf.logAppendEntry("[%d] update commitIndex to %d", rf.me, rf.commitIndex)
		rf.applyCond.Signal()
	}
	reply.Success = true
	return nil
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.logInstallSnapshot("[%d] receive snapshot from %d, lastIncludedIndex=%d, lastIncludedTerm=%d", rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	// reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return nil
	}

	termChanged := false
	saveSnapshot := false
	defer func() {
		if termChanged && !saveSnapshot {
			err := rf.persist()
			if err != nil {
				panic(err.Error())
			}
		}
		rf.resetTimeout()
	}()
	// rules for all servers in Figure 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
		termChanged = true
	}

	lastIncludedIndex := args.LastIncludedIndex
	lastIncludedTerm := args.LastIncludedTerm
	if rf.lastIncludedIndex >= lastIncludedIndex || rf.lastIncludedTerm > lastIncludedTerm ||
		rf.lastApplied >= lastIncludedIndex || rf.lastAppliedTerm > lastIncludedTerm {
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
	saveSnapshot = true
	messages := []applyMsg{
		{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		},
	}
	order := rf.nextOrder
	rf.nextOrder++
	rf.mu.Unlock()

	go rf.sendApplyMessages(order, messages)
	return nil
}

// The ticker go routine starts a new election if this peer hasn't received heartbeat recently.
func (rf *Raft) electionTicker() {
	rf.resetTimeout()
	for {
		rf.mu.Lock()
		electionTimeout := rf.timeToElection
		if currentMilli() >= electionTimeout {
			if rf.state != leader {
				go rf.newElection()
			}
			electionTimeout = rf.resetTimeout()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(electionTimeout-currentMilli()) * time.Millisecond)
	}
}

func (rf *Raft) newElection() {
	rf.mu.Lock()
	newTerm := rf.currentTerm + 1
	me := rf.me
	rf.currentTerm = newTerm
	rf.votedFor = me
	err := rf.persist()
	if err != nil {
		panic(err.Error())
	}
	peers := rf.peers
	majorityVotes := ((len(peers) + 1) >> 1) + 1
	if majorityVotes < 2 {
		rf.state = leader
		rf.commitIndex = rf.lastIncludedIndex + len(rf.log)
		rf.mu.Unlock()
		rf.logRequestVote("[%d] is leader in term %d, because there are no other servers!", me, newTerm)
		return
	}
	rf.state = candidate

	logCount := len(rf.log)
	var lastLogTerm = rf.lastIncludedTerm
	if logCount > 0 {
		lastLogTerm = rf.log[logCount-1].Term
	}
	lastLogIndex := rf.lastIncludedIndex + logCount
	rf.mu.Unlock()
	rf.logRequestVote("[%d] start a new election in term %d", me, newTerm)
	args := &RequestVoteArgs{
		Term:         newTerm,
		CandidateId:  me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.sendVoteRequestsAndHandleReply(args, peers, majorityVotes)

}

// heartbeat sends heartbeat message to server in termN
// leaders starts a heartbeat routine for each follower when it becomes a leader in termN
func (rf *Raft) heartbeat(server int, termN int) {
	for true {
		for {
			rf.mu.Lock()
			if rf.state != leader || rf.currentTerm != termN {
				rf.mu.Unlock()
				return
			}
			lastSentTime := rf.heartbeatSent[server]
			pastMills := currentMilli() - lastSentTime
			if pastMills >= heartbeatInterval {
				rf.heartbeatSent[server] = currentMilli()
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			time.Sleep(time.Duration(heartbeatInterval-pastMills) * time.Millisecond)
		}
		rf.sendHeartbeatMessage(server, termN)
		time.Sleep(heartbeatInterval * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeatMessage(server int, term int) {
	rf.mu.Lock()
	if rf.state != leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[server]
	if nextIndex <= rf.lastIncludedIndex {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.storage.GetSnapshot(),
		}
		rf.mu.Unlock()
		go rf.sendSnapshotAndHandleReply(server, term, args)
		return
	}
	j := nextIndex - rf.lastIncludedIndex - 1
	logCount := len(rf.log)
	prevLogTerm := rf.lastIncludedTerm
	if j > 0 && logCount >= j {
		prevLogTerm = rf.log[j-1].Term
	}
	count := logCount - j
	var entries []LogEntry
	if count > 0 {
		entries = make([]LogEntry, count)
		for i := 0; i < count; i++ {
			entries[i] = rf.log[j]
			j++
		}
	}

	lastLogIndex := rf.lastIncludedIndex + logCount
	prevLogIndex := nextIndex - 1

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
	go rf.sendEntriesAndHandleReply(server, lastLogIndex, term, args)
}

// majorityCommitIndex
// find the Nth log, which a majority of matchIndex[i] >= N and its term is leader's term
func majorityCommitIndex(rf *Raft) int {
	totalServers := len(rf.peers) + 1
	arr := make([]int, totalServers)
	i := 0

	for i < totalServers-1 {
		arr[i] = rf.matchIndex[i]
		i++
	}
	arr[i] = rf.lastIncludedIndex + len(rf.log)
	sort.Ints(arr)
	index := rf.commitIndex
	midCommit := arr[totalServers>>1]
	// according to Figure 8
	// the preceding logs can be overwritten by other leaders
	// unless leader replicates an entry from its current term on a majority of the servers before crashing
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
func backupIndex(reply *AppendEntriesReply, log []LogEntry, prevLogIndex int, lastIncludedIndex int) int {
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
