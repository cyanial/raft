package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
//
// rf.Start(command interface{}) (index, term isleader)
//   start agreement on a new log entry
//
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
//
// ApplyMsg
//  each time a new entry is committed to the log, each Raft peer
//  should send an ApplyMsg to the service (to tester)
//  in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyanial/raft/labrpc"
)

type RaftType string

const (
	Leader    RaftType = "Leader"
	Follower  RaftType = "Follower"
	Candidate RaftType = "Candidate"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server,  via the applyCh passed to Make(). Set
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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// log entry
//
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state             RaftType
	heartbeatTimeout  time.Duration
	lastHeartbeatTime time.Time

	currentTerm int
	votedFor    int
	log         []*LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []*LogEntry
	matchIndex []*LogEntry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.state == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see papaer's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e L= labgob.NewEncoder
}

//
// restore previously persisted state.
//
func (rt *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot. Only do so if Raft hasn't
// have more recent info since it commnunicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludeTerm int, lastIncludeIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

//
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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
	term := args.Term
	candidateId := args.CandidateId
	// lastLogIndex := args.LastLogIndex
	// lastLogTerm := args.LastLogTerm

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.votedFor = candidateId
	rf.state = Follower

	reply.Term = term
	reply.VoteGranted = true

	// 2. If votedFor is null or candidateId, and candidate's log is at least
	//    as up-to-date as receiver'log, grant vote.

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
}

//
// example AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// Your code here (2A, 2B).
	term := args.Term
	// leaderId := args.LeaderId
	// prevLogIndex := args.PrevLogIndex
	// prevLogTerm := args.PrevLogTerm
	// entries := args.entries
	// leaderCommit := args.leaderCommit

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.currentTerm = term
	rf.state = Follower
	rf.lastHeartbeatTime = time.Now()

	reply.Term = term
	reply.Success = true

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose
	//    term matches prevLogTerm

	// 3. If an existing entry conflicts with a new one (same index but differnt
	//    terms), delete the existing entry and all that follow it

	// 4. Append any new entries not already in log

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit,
	// 	  index of last new entry)

}

//
// the service using Raft (e.g. a k/v server) wants to start
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// go process()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perphaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// hould call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// random sleep time. (150ms-300ms paper recommanded)
		// a little bit longer than recommanded (300ms-450ms)
		randomTime := time.Duration(rand.Intn(151)+300) * time.Millisecond
		time.Sleep(randomTime)

		// heartbeat was received in the sleep period

		rf.mu.Lock()
		if rf.lastHeartbeatTime.Add(randomTime).After(time.Now()) {
			DPrintf("%d %q receive a heartbeat in last period time", rf.me, rf.state)
		} else {
			DPrintf("rf.ticker() %d %q\n", rf.me, rf.state)
			if rf.state == Follower {
				// start election
				rf.state = Candidate
				rf.currentTerm++
				go rf.startElection()
			}
		}
		rf.mu.Unlock()
	}
}

//
// start an election
//
func (rf *Raft) startElection() {

	args := &RequestVoteArgs{}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("rf.startElection() %d %q\n", rf.me, rf.state)

	if rf.state != Candidate {
		return
	}

	mu_votes := sync.Mutex{}
	votes, voters := 1, len(rf.peers)
	rf.votedFor = rf.me

	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = -1
	args.LastLogTerm = -1

	receiveMajority := make(chan bool, 1)

	for i := 0; i < int(voters); i++ {
		if i == rf.me {
			continue
		}
		go func(id int, rf *Raft) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(id, args, reply) {
				DPrintf("request vote call failed %%d %%q\n")
				return
			}
			if reply.VoteGranted {
				mu_votes.Lock()
				votes++
				if votes > voters/2 {
					receiveMajority <- true
				}
				mu_votes.Unlock()
			} else {
				// To-do:
				// turn to follower and update with
				// reply.Term
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
				}
				rf.mu.Unlock()
			}
		}(i, rf)
	}

	go rf.checkVotes(receiveMajority)
}

func (rf *Raft) checkVotes(receiveMajority chan bool) {

	randomTime := time.Duration(rand.Intn(151)+300) * time.Millisecond
	select {
	case <-receiveMajority:
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Candidate {
			return
		}
		rf.state = Leader
		// send heartbeat
		go rf.sendHeartbeat()
	case <-time.After(randomTime):
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Candidate {
			return
		}
		rf.currentTerm++
		go rf.startElection()
	}
}

// The triggerHeartbeat() send heartbeat periodically if it is a leader
func (rf *Raft) triggerHeartbeat() {
	for rf.killed() == false {
		time.Sleep(rf.heartbeatTimeout)

		rf.mu.Lock()

		defer DPrintf("rf.triggerHeartBeat, %d, %q\n", rf.me, rf.state)
		if rf.state == Leader {
			// send heartbeat
			go rf.sendHeartbeat()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartbeat() {

	args := &AppendEntriesArgs{}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = -1
	args.PrevLogTerm = -1
	args.Entries = nil // empty for heartbeat
	args.LeaderCommit = rf.commitIndex

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(id int) {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(id, args, reply)
			if !ok {
				DPrintf("send append entries false")
			}
			if reply.Success {
				return
			}

			if reply.Success == false {
				// rf.mu.Lock()
				// rf.state = Follower
				// rf.currentTerm = reply.Term
				// rf.mu.Unlock()
				rf.setStateAndTerm(Follower, reply.Term)
			}
		}(i)
	}
}

func (rf *Raft) setStateAndTerm(state RaftType, currentTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = state
	rf.currentTerm = currentTerm
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C)
	rf.state = Follower
	rf.heartbeatTimeout = 120 * time.Millisecond // must smaller than election timeout
	rf.lastHeartbeatTime = time.Unix(0, 0)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []*LogEntry{}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = []*LogEntry{}
	rf.matchIndex = []*LogEntry{}

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// send heartbeat package if it's a leader
	go rf.triggerHeartbeat()

	DPrintf("Init Raft: %d %q\n", rf.me, rf.state)
	return rf
}
