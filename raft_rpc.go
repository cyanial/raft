package raft

import (
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// Invoked by candidates to gather votes

	// candidate's term
	Term int

	// candidate requesting vote
	CandidateId int

	// index of candidate's last log entry
	LastLogIndex int

	// term of candidate's last log entry
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// currentTerm, for candidate to update itself
	Term int

	// true means candidate received vote
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// args.Term > rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		if rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			rf.votedFor = -1
			reply.VoteGranted = false
		}
		return
	}

	// 2. If votedFor is null or candidateId, and candidate's log is at least
	//    as up-to-date as receiver'log, grant vote.
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId &&
		rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		rf.votedFor = -1
	}

}

//
// example AppendEntires RPC args structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {

	// leader's term
	Term int

	// follower can redirect clients
	LeaderId int

	// index of log entry immediately preceding new ones
	PrevLogIndex int

	// term of PrevLogIndex entry
	PrevLogTerm int

	// log entries to store (empty for heartbeat; may send more than one for
	// efficiency)
	Entries []LogEntry

	// leader's commitIndex
	LeaderCommit int
}

//
// example AppendEntires RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {

	// currentTerm, for leader to update itself
	Term int

	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool

	// Optimized:
	// Term of the conflicting entry and the first index it stores for that term
	// ConflictTerm int
	// FirstIndex   int
}

//
// example AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.lastHeartbeatTime = time.Now()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	rf.state = Follower
	rf.votedFor = args.LeaderId

	// args.Term == rf.currentTerm
	// no new this is heartbeat
	// if args.PrevLogIndex == len(rf.log)-1 && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = true
	// 	return
	// }

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose
	//    term matches prevLogTerm
	// 3. If an existing entry conflicts with a new one (same index but differnt
	//    terms), delete the existing entry and all that follow it

	// DPrintf("\t\t\t %d, %q, prevLogIndex: %d, pregLogTerm: %d\n", rf.me, rf.state, args.PrevLogIndex, args.PrevLogTerm)
	// DPrintf("\t\t\t\t len(rf.log): %d, rf.log[prevlogIndex].Term: ", len(rf.log))
	// DPrintf("\t\t\t\t log: %#v\n", rf.log)
	if args.PrevLogIndex >= len(rf.log) || (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 4. Append any new entries not already in log
	// at this point -
	//   entry at PrevLogIndex.Term is equal to PrevLogTerm
	// Find an insertion point -
	//   where there's a term mismatch between the existing long starting at
	//   PrevLogIndex+1 and the new entries sent in the RPC.
	logInsertIndex := args.PrevLogIndex + 1
	newEntriesIndex := 0
	for {
		if logInsertIndex >= len(rf.log) || newEntriesIndex >= len(args.Entries) {
			break
		}
		if rf.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
			break
		}
		logInsertIndex++
		newEntriesIndex++
	}
	if newEntriesIndex < len(args.Entries) {
		// DPrintf("\t\t\t %d append entries %#v\n", rf.me, args.Entries)
		rf.log = append(rf.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit,
	// 	  index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.newCommitCh <- struct{}{}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass *reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in wichi servers
// may be unreachable, and in which request and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return. Thus there
// is no need to implement your own timeous around Call().
//
// look at the comments in ../labrpc/labrpc.go for a more details.
//
// if you're having trouble getting RPC to work, check taht you've
// capitalized all field name in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
