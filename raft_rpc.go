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

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	defer rf.persist()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// 2. If votedFor is null or candidateId, and candidate's log is at least
	//    as up-to-date as receiver'log, grant vote.
	reply.Term = args.Term
	reply.VoteGranted = false

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {

		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeartbeatTime = time.Now()
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
	ConflictTerm  int
	ConflictIndex int
}

//
// example AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	defer rf.persist()

	// - (All Servers) If RPC request or response contains term T > currentTerm:
	//   set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// - (Candidates) If AppendEntries RPC received from new leader: convert
	//   to follower
	if rf.state == Candidate {
		rf.becomeFollower(args.Term)
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose
	//    term matches prevLogTerm
	// 3. If an existing entry conflicts with a new one (same index but differnt
	//    terms), delete the existing entry and all that follow it
	if args.PrevLogIndex >= rf.logSize() || (rf.logAt(args.PrevLogIndex).Term != args.PrevLogTerm) {
		reply.Term = args.Term
		reply.Success = false

		// - If a follower does not have prevLogIndex in its log, it should return
		//   with conflictIndex = len(log) and conflictTerm = None
		// - If a follower does have prevLogIndex in its log, but the term does not
		//   match, it should return conflictTerm = log[prevLogIndex].Term, and
		//   then search its log for the first index whose entry has term equal
		//   to conflictTerm
		if args.PrevLogIndex >= rf.logSize() {
			reply.ConflictIndex = rf.logSize()
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.logAt(args.PrevLogIndex).Term
			for i, l := range rf.log {
				if l.Term == reply.ConflictTerm {
					reply.ConflictIndex = i + rf.logBase
					break
				}
			}
			// rf.log = rf.log[:reply.ConflictIndex]
		}
		rf.lastHeartbeatTime = time.Now()
		return
	}

	// 4. Append any new entries not already in log
	rf.log = append(rf.log[:args.PrevLogIndex+1-rf.logBase], args.Entries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit,
	// 	  index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logSize()-1)
		rf.newCommitCh <- struct{}{}
	}

	rf.lastHeartbeatTime = time.Now()
	reply.Term = rf.currentTerm
	reply.Success = true
}

//
// example InstallSnapshot RPC args structure.
// field names must start with capital letters!
//
type InstallSnapshotArgs struct {

	// Leader's term
	Term int

	// So follower can redirect clients
	LeaderId int

	// The snapshot replaces all entries up
	// through and including this index
	LastIncludedIndex int

	// Term of lastIncludeIndex
	LastIncludedTerm int

	// Byte offset where chunk is positioned
	// in the snapshot file
	// Send the entire snapshot in a single InstallSnapshot RPC.
	// Don't implement Figure13's offset mechanism for splitting
	// up the snapshot.
	// Offset int

	// Raw bytes of the snapshot chunk, starting
	// at offset
	Data []byte

	// true if this is the last chunk (impl with no offset)
	// Done bool
}

//
// example InstallSnapshot RPC reply structure.
// field names must start with capital letters!
//
type InstallSnapshotReply struct {

	// CurrentTerm, for leader to update itself.
	Term int
}

//
// example InstallSnapshot RPC handler
// Invoked by leader to send chunks of a snapshot
// to a follower. Leaders always send chunks in order.
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		// rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm || rf.state == Candidate {
		rf.becomeFollower(args.Term)
		rf.persist()
	}

	reply.Term = args.Term

	if args.LastIncludedIndex <= rf.logBase {
		// snapshot is not up-to-date
		// rf.mu.Unlock()
		return
	}

	rf.lastHeartbeatTime = time.Now()

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

	if args.LastIncludedIndex < rf.logSize() &&
		rf.logAt(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		rf.log = append([]LogEntry(nil), rf.log[args.LastIncludedIndex-rf.logBase:]...)
	} else {
		rf.log = append([]LogEntry(nil), LogEntry{Term: args.LastIncludedTerm})
	}

	rf.logBase = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	rf.persistStateAndSnapshot(args.Data)

	// 6. If existing log entry has same index and term as snapshot's
	//    last included entry, retain log entries following it and
	//    reply
	// 7. Discard the entire log
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
