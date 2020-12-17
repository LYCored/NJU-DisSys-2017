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
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

type Log struct {
	Term    int
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
// CurrentTerm: latest term server has seen (initialized to 0 on first boot, increases monotonically)
// VotedFor: candidateId that received vote in current term (or null if none)
// Log: log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
// CommitIndex: index of highest log entry known to be committed (initialized to 0, increases monotonically)
// LastApplied: index of highest log entry applied to state machine (initialized to 0, increases monotonically)
// NextIndex: for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
// MatchIndex: for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers
	CurrentTerm int
	VotedFor    int
	Logs        []Log
	// Volatile state on all servers
	CommitIndex int
	LastApplied int
	// Volatile state on leaders
	LeaderFlag bool
	NextIndex  []int
	MatchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here.
	return rf.CurrentTerm, rf.LeaderFlag
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
// Term: candidate’s term
// CandidateID: candidate requesting vote
// LastLogIndex: index of candidate’s last log entry
// LastLogTerm: term of candidate’s last log entry
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// Term: currentTerm, for candidate to update itself
// VoteGranted: true means candidate received vote
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	lastIndex := -1
	lastTerm := -1
	if len(rf.Logs) > 0 {
		lastIndex = len(rf.Logs) - 1
		lastTerm = rf.Logs[lastIndex].Term
	}
	if args.Term >= rf.CurrentTerm && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateID) && (args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)) {
		reply.Term = rf.CurrentTerm // TODO:或许不该有
		reply.VoteGranted = true
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.CandidateID
		rf.LeaderFlag = false
	} else { // TODO:如果不接受这个Leader应该干什么？
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// My code
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

// My code
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// My code
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
	} else {
		rf.CurrentTerm = args.Term
		rf.LeaderFlag = false
		reply.Success = true
		if len(args.Entries) > 0 {
			if args.PrevLogIndex >= 0 && rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.Success = false
			} else {
				rf.Logs = append(rf.Logs[:args.PrevLogIndex+1], args.Entries...)
				rf.CommitIndex = len(rf.Logs)
			}
		}
	}
}

// My code
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	entry := Log{
		Term:    rf.CurrentTerm,
		Command: command,
	}
	rf.Logs = append(rf.Logs, entry)
	rf.CommitIndex += 1
	index := rf.CommitIndex
	term := rf.CurrentTerm
	isLeader := rf.LeaderFlag

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Logs = make([]Log, 0)
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
