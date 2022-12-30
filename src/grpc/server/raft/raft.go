package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is Leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"kvraft/src/rpc"
	"kvraft/src/safegob"
	"kvraft/src/storage"
	"log"
	"math/rand"
	"net/http"
	rpc2 "net/rpc"
	"sort"
	"strconv"
	"strings"

	//	"bytes"
	"sync"
	"time"
)

// 为了方便使用日志进行debug或者进行测试, 使用常量开关来控制日志的打印
const (
	Log2AEnabled = true
	Log2BEnabled = true
	Log2CEnabled = true
	Log2DEnabled = true
)

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

func log2A(format string, v ...interface{}) {
	if Log2AEnabled {
		log.Printf(format, v...)
	}
}

func log2B(format string, v ...interface{}) {
	if Log2BEnabled {
		log.Printf(format, v...)
	}
}

func log2C(format string, v ...interface{}) {
	if Log2CEnabled {
		log.Printf(format, v...)
	}
}

func log2D(format string, v ...interface{}) {
	if Log2DEnabled {
		log.Printf(format, v...)
	}
}

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// LogEntry
// log object
type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu      sync.Mutex       // Lock to protect shared access to this peer's state
	peers   []*rpc.Peer      // RPC end points of all peers
	storage *storage.Storage // Object to hold this peer's persisted state
	me      int              // this peer's index into peers[]
	dead    int32            // set by Kill()

	currentTerm int        // persistent, the latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // persistent, candidateId that received vote in current term (or null if none)
	log         []LogEntry // persistent, log entries (first index is 1)

	commitIndex      int           // index of the highest log entry known to be committed (initialized to 0)
	lastApplied      int           // index of the highest log entry known to be applied to state machine (initialized to 0)
	lastAppliedTerm  int           // term of lastApplied
	role             string        // server role
	applyCh          chan ApplyMsg // for raft to send committed log and snapshot
	electionTimeout  int64         // time to start a new election, milliseconds
	minMajorityVotes int           // the minimum number of votes required to become a leader

	// for 2D
	lastIncludedIndex int    // persistent
	lastIncludedTerm  int    // persistent
	snapshot          []byte // in-memory snapshot

	// leader state
	nextIndex     []int   // index of the next log entry send to the server (initialized to 1)
	matchIndex    []int   // index of highest log entry known to be replicated on server
	heartbeatSent []int64 // time of last sent heartbeat for each server (millisecond)
}

// return currentTerm and whether this server
// believes it is the Leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term = rf.currentTerm
	var isLeader = strings.Compare(rf.role, Leader) == 0

	return term, isLeader
}

func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := safegob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.lastIncludedIndex) != nil ||
		e.Encode(rf.lastIncludedTerm) != nil ||
		e.Encode(rf.log) != nil {
		log.Fatalf("[%d] encode raft state failed!", rf.me)
	}
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	state := rf.getRaftState()
	rf.storage.SaveRaftState(state)
	log2C("[%d] persist raft state success!", rf.me)
}

//
// restore previously persisted state.
//
func (rf *Raft) readRaftState(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	buf := bytes.NewBuffer(data)
	d := safegob.NewDecoder(buf)

	var currentTerm int
	var votedFor int
	var logEntries []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	var err error
	if err = d.Decode(&currentTerm); err != nil {
		log.Println("[", rf.me, "]", " decode currentTerm failed!\n errorInfo: ", err)
		return
	}
	if err = d.Decode(&votedFor); err != nil {
		log.Println("[", rf.me, "]", " decode votedFor failed!\n errorInfo: ", err)
		return
	}
	if err = d.Decode(&lastIncludedIndex); err != nil {
		log.Println("[", rf.me, "]", " decode lastIncludedIndex failed!\n errorInfo: ", err)
		return
	}
	if err = d.Decode(&lastIncludedTerm); err != nil {
		log.Println("[", rf.me, "]", " decode lastIncludedTerm failed!\n errorInfo: ", err)
		return
	}
	if err = d.Decode(&logEntries); err != nil {
		log.Println("[", rf.me, "]", " decode logEntries failed! errorInfo: ", err)
		return
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
	log2C("[%d] restore raft state from persist success!", rf.me)

}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// had more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied > lastIncludedIndex {
		log2D("[%d] has applied entries after the snapshot's lastIncludedIndex!, lastApplied = %d, lastIncludedIndex = %d", rf.me, rf.lastApplied, lastIncludedIndex)
		return false
	}
	if rf.lastAppliedTerm > lastIncludedTerm {
		log2D("[%d] has applied entries after the snapshot's lastIncludedTerm!, lastAppliedTerm = %d, lastIncludedTerm = %d", rf.me, rf.lastIncludedTerm, lastIncludedTerm)
		return false
	}
	old := rf.lastIncludedIndex
	reservedIndex := lastIncludedIndex - old
	rf.snapshot = snapshot
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastApplied = lastIncludedIndex
	if reservedIndex >= len(rf.log) {
		rf.log = []LogEntry{}
	} else {
		rf.log = rf.log[reservedIndex:]
	}
	state := rf.getRaftState()
	rf.storage.SaveStateAndSnapshot(state, snapshot)
	log2D("[%d] switch to new snapshot, lastIncludedIndex=%d, lastIncludedTerm=%d, log length=%d", rf.me, lastIncludedIndex, lastIncludedTerm, len(rf.log))
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	old := rf.lastIncludedIndex
	if old >= index {
		return
	}
	discardedLogIndex := index - old - 1
	rf.snapshot = snapshot
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[discardedLogIndex].Term
	if discardedLogIndex+1 >= len(rf.log) {
		rf.log = []LogEntry{}
	} else {
		rf.log = rf.log[discardedLogIndex+1:]
	}
	state := rf.getRaftState()
	rf.storage.SaveStateAndSnapshot(state, snapshot)
	log2D("{%d] make a new snapshot, oldLastIncludedIndex=%d, newLastIncludedIndex=%d, trim log after %d", rf.me, old, index, discardedLogIndex)
}

// RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
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
	// Your data here (2A).
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

// RequestVote
// RPC handler for RequestVote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return nil
	}

	// rules for all servers in Figure 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate(args, rf) {
		log2A("[%d] vote for %d in term %d", rf.me, args.CandidateId, args.Term)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionTimeout = randomElectionTimeout()
	}
	rf.persist()
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
// 这里我选择在 return 之前进行重置，因为重置是在持有锁的情况下进行的，更精确
// 只要在超时前处理过 AppendEntries，
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	totalLogLen := rf.lastIncludedIndex + len(rf.log) + 1
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XLen = totalLogLen
	log2A("[%d] receive append rpc from %d in term %d", rf.me, args.LeaderId, args.Term)

	// reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return nil
	}

	// rules for all servers in Figure 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
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

	// PrevLogIndex < totalLogLen，会有三种种情况
	// 1. PrevLogIndex > lastIncludedIndex
	// 2. PrevLogIndex < lastIncludedIndex
	// 3. PrevLogIndex == lastIncludedIndex
	// 由于网络延迟等原因，会出现第2/3种情况；第二种情况中，我们无法比较这两个log的Term; 但是这种情况无需比较，因为Snapshot中的log都是committed，
	// 根据Figure3的特性可知, leader 肯定包含有最新的 committed log，因此 leader 和 follower 至少会在 lastIncludedIndex 上有相同的 log
	// prevLogIndex == -1 时，说明 leader 发送过来的 log 都是在 lastIncludedIndex 之后的, idx=0即可
	// prevLogIndex < -1 时，则只比较在 lastIncludedIndex 之后的 log, idx 需要加上偏移量
	idx := 0
	i := 0
	prevLogIndex := args.PrevLogIndex - rf.lastIncludedIndex - 1
	if prevLogIndex < -1 {
		idx -= prevLogIndex + 1
	} else if prevLogIndex >= 0 {
		// if term of log entry in prevLogIndex not match prevLogTerm
		// set XTerm to term of the log
		// set XIndex to the first entry in XTerm
		// reply false (§5.3)
		if rf.log[prevLogIndex].Term != args.PrevLogTerm {
			reply.XTerm = rf.log[prevLogIndex].Term
			for prevLogIndex > 0 {
				if rf.log[prevLogIndex-1].Term != reply.XTerm {
					break
				}
				prevLogIndex--
			}
			reply.XIndex = prevLogIndex + rf.lastIncludedIndex + 1
			rf.electionTimeout = randomElectionTimeout()
			return nil
		}
		// match, set i to prevLogIndex + 1, prepare for comparing the following logs
		i = prevLogIndex + 1
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	truncated := false
	for ; i < len(rf.log) && idx < len(args.Entries); i++ {
		if rf.log[i].Term == args.Entries[idx].Term {
			idx++
			continue
		}
		rf.log = rf.log[0:i]
		log2B("[%d] exists conflict entry in index %d, delete the entry and all after that", rf.me, i)
		truncated = true
		break
	}
	appended := idx < len(args.Entries)
	// append any new entries not already in the log
	if idx < len(args.Entries) {
		count := len(args.Entries) - idx
		for idx < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[idx])
			idx++
		}
		log2B("[%d] append %d new entries", rf.me, count)
	}

	if truncated || appended {
		rf.persist()
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		maxLogIndex := rf.lastIncludedIndex + len(rf.log)
		if args.LeaderCommit < maxLogIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = maxLogIndex
		}
		log2B("[%d] update commitIndex to %d", rf.me, rf.commitIndex)
	}
	reply.Success = true
	rf.electionTimeout = randomElectionTimeout()
	return nil
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return nil
	}

	// rules for all servers in Figure 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	log2D("[%d] receive snapshot from %d, lastIncludedIndex=%d, lastIncludedTerm=%d", rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.applyCh <- applyMsg
	rf.electionTimeout = randomElectionTimeout()
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

//
// the service using Raft (e.g. a k/v server) wants to start
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
	// Your code here (2B) .
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.role == Leader
	if isLeader {
		index = rf.lastIncludedIndex + 1 + len(rf.log)
		log2B("[%d] is leader, start agreement on log index %d", rf.me, index)
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.persist()
		if rf.minMajorityVotes < 2 {
			rf.commitIndex++
		}
		for server := range rf.peers {
			if server != rf.me {
				rf.heartbeatSent[server] = currentMilli()
				go rf.sendHeartBeatMsg(server, term)
			}
		}
	}
	return index, term, isLeader
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	time.Sleep(1 * time.Second)
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
	rf.persist()
	rf.role = Candidate
	votes := 1
	if rf.minMajorityVotes < 2 {
		rf.role = Leader
		rf.mu.Unlock()
		return
	}
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
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			log2A("[%d] request vote from %d in term %d", rf.me, server, args.Term)
			// Call() sends a request and waits for a reply. If a reply arrives
			// within a timeout interval, Call() returns true; otherwise
			// Call() returns false. Thus Call() may not return for a while.
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// rules for all servers in Figure 2
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}
				// term 或 role 发生改变时，当前 election 作废
				if done || rf.role != Candidate || rf.currentTerm != args.Term {
					return
				}
				if reply.VoteGranted {
					votes++
					log2A("[%d] got vote from %d in term %d", rf.me, server, args.Term)
					if votes >= rf.minMajorityVotes {
						log2A("[%d] got majority votes in term %d, [%d] is leader!", rf.me, args.Term, rf.me)
						done = true
						rf.role = Leader
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						rf.heartbeatSent = make([]int64, len(rf.peers))
						totalLogCount := rf.lastIncludedIndex + len(rf.log) + 1
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = totalLogCount
							rf.matchIndex[i] = 0
						}
						rf.electionTimeout = randomElectionTimeout()
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								continue
							}
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

// majorityCommitIndex
// find the N, that a majority of matchIndex[i] >= N
func majorityCommitIndex(rf *Raft) int {
	servers := len(rf.peers)
	arr := make([]int, servers)
	rf.matchIndex[rf.me] = rf.lastIncludedIndex + len(rf.log)
	for i := 0; i < servers; i++ {
		arr[i] = rf.matchIndex[i]
	}
	sort.Ints(arr)
	index := rf.commitIndex
	midCommit := arr[servers>>1]
	for i := midCommit; i >= rf.commitIndex+1; i-- {
		if rf.log[i-rf.lastIncludedIndex-1].Term == rf.currentTerm {
			return i
		}
	}
	return index
}

// backupIndex
// called when AppendEntries fails
// determine the nextIndex according to the condition
// this was mentioned in https://www.youtube.com/watch?v=4r8Mz3MMivY Backup Faster section
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

// applyLog
// a goroutine to apply committed log
func (rf *Raft) applyLog() {
	for {
		rf.mu.Lock()
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		if lastApplied < commitIndex {
			startIndex := lastApplied + 1
			endIndex := commitIndex
			commandIndex := startIndex
			index := startIndex - rf.lastIncludedIndex - 1
			var logTerm int
			for commandIndex <= endIndex {
				logTerm = rf.log[index].Term
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[index].Command,
					CommandIndex: commandIndex,
					CommandTerm:  logTerm,
				}

				rf.applyCh <- msg
				commandIndex++
				if commandIndex%10 == 0 {
					break
				}
				index++
				//log.Printf("[%d] applyLog, commandIndex=%d, endIndex=%d, loglength=%d", rf.me, commandIndex, endIndex, len(rf.log))
			}
			rf.lastApplied = commandIndex - 1
			rf.lastAppliedTerm = logTerm
			log2B("[%d] apply log in [%d, %d]", rf.me, startIndex, commandIndex-1)
		}
		//if rf.lastApplied < rf.commitIndex {
		//	commandIndex := rf.lastApplied + 1
		//	index := commandIndex - rf.lastIncludedIndex - 1
		//	msg := ApplyMsg{
		//		CommandValid: true,
		//		Command:      rf.log[index].Command,
		//		CommandIndex: commandIndex,
		//	}
		//	rf.applyCh <- msg
		//	rf.lastApplied++
		//	rf.lastAppliedTerm = rf.log[index].Term
		//}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) applySnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastIncludedIndex == -1 {
		return
	}
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotIndex: rf.lastIncludedIndex,
		SnapshotTerm:  rf.lastIncludedTerm,
	}
	rf.applyCh <- msg
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
				Data:              rf.snapshot,
			}
			matchIndex := rf.matchIndex[server]
			rf.mu.Unlock()
			go func(args InstallSnapshotArgs, nextIndex int, matchIndex int) {
				log2D("[%d] send snapshot to %d, last included index is %d", args.LeaderId, server, args.LastIncludedIndex)
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
						rf.persist()
						return
					}
					if rf.role != Leader || rf.currentTerm != term {
						return
					}
					if rf.nextIndex[server] > nextIndex || rf.matchIndex[server] > matchIndex {
						return
					}
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
		log2A("[%d] send append rpc to %d in term %d", args.LeaderId, server, args.Term)
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
				rf.persist()
				return
			}
			if rf.role != Leader || rf.currentTerm != term {
				return
			}
			if reply.Success {
				if rf.nextIndex[server] > lastIndex {
					return
				}
				rf.nextIndex[server] = lastIndex + 1
				rf.matchIndex[server] = lastIndex
				log2B("[%d] receive success append reply from %d, last append index is %d", rf.me, server, lastIndex)
				index := majorityCommitIndex(rf)
				if rf.commitIndex < index {
					rf.commitIndex = index
					log2B("[%d] has replicated log in index %d on majority servers, update commitIndex to %d", rf.me, index, index)
				}
			} else {
				rollbackIndex := backupIndex(reply, rf.log, args.PrevLogIndex, rf.lastIncludedIndex)
				if rollbackIndex <= rf.nextIndex[server] {
					rf.nextIndex[server] = rollbackIndex
					log2B("[%d] append to %d failed, nextIndex back up to %d", rf.me, server, rollbackIndex)
				}
			}
		}
	}(args, lastLogIndex)
}

func StartRaft(serverAddresses []string, me int, storage *storage.Storage, applyCh chan ApplyMsg) *Raft {
	var raftAddresses []string
	for i := 1; i < 2; i++ {
		raftAddresses = append(raftAddresses, "localhost:808"+strconv.Itoa(i))
	}
	rf := &Raft{}
	rf.peers = make([]*rpc.Peer, len(raftAddresses))
	for i := 0; i < len(raftAddresses); i++ {
		rf.peers[i] = rpc.MakePeer(raftAddresses[i])
	}
	rf.storage = storage
	rf.me = me
	rf.applyCh = applyCh

	// initialize raft state according to Figure 2
	rf.dead = 0
	rf.role = Follower
	rf.minMajorityVotes = (len(raftAddresses) / 2) + 1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = 0
	var logEntries []LogEntry
	logEntries = append(logEntries, LogEntry{
		Term:    0,
		Command: nil,
	})
	rf.log = logEntries

	// every server has a default log entry in index 0
	// so index 0 is committed
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastAppliedTerm = 0

	// initialize from state persisted before a crash
	rf.readRaftState(storage.ReadRaftState())
	rf.snapshot = storage.ReadSnapshot()

	// start ticker goroutine to start elections
	rf.electionTimeout = randomElectionTimeout()
	go rf.ticker()

	// start apply goroutine to apply log
	go rf.applyLog()
	// start rpc server
	server := rpc2.NewServer()
	err := server.Register(rf)
	if err != nil {
		log.Fatalln("start K/V server fail! errorInfo:", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/", server)
	go http.ListenAndServe(raftAddresses[me], server)
	return rf
}
