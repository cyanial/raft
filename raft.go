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
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	applyCh     chan ApplyMsg
	newCommitCh chan struct{}

	state             RaftType
	heartbeatTimeout  time.Duration
	lastHeartbeatTime time.Time

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) Report() {
	// someone has already lock
	DPrintf("id:%d, %10s, term: %d, commitIndex: %d, lastApplied: %d %v %v\n",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
	DPrintf("------------- id: %d, log: %d\n", rf.me, len(rf.log[1:]))
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// is not leader, return false
	if rf.state != Leader {
		return -1, -1, false
	}

	index := len(rf.log)
	term := rf.currentTerm

	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
	})
	DPrintf("\t\t\t Start new command %v\n", command)

	return index, term, true
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
		// a little bit longer than recommanded (400-550ms)
		ms := 400 + (rand.Int63() % 150)
		wait_time := time.Duration(ms) * time.Millisecond
		time.Sleep(wait_time)

		// heartbeat was received in the sleep period

		rf.mu.Lock()
		if rf.lastHeartbeatTime.Add(wait_time).After(time.Now()) {
			// DPrintf("%d %q receive a heartbeat in last period time", rf.me, rf.state)
		} else {
			if rf.state == Follower {
				// start election
				rf.state = Candidate
				rf.currentTerm++
				go rf.startElection(rf.currentTerm)
			}
		}
		rf.mu.Unlock()
	}
}

//
// start an election
//
func (rf *Raft) startElection(electionTerm int) {

	args := &RequestVoteArgs{}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate || rf.currentTerm != electionTerm {
		return
	}

	DPrintf("\t\t\t startElection id:%d, term: %d %q\n", rf.me, rf.currentTerm, rf.state)

	mu_votes := sync.Mutex{}
	votes, voters := 1, len(rf.peers)
	rf.votedFor = rf.me

	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()

	receiveMajority := make(chan bool, 1)
	once := sync.Once{}

	for i := 0; i < int(voters); i++ {
		if i == rf.me {
			continue
		}
		go func(id int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(id, args, reply)
			if !ok {
				// DPrintf("request vote call failed %%d %%q\n")
				return
			}
			if reply.VoteGranted {
				mu_votes.Lock()
				votes++
				if votes > voters/2 {
					once.Do(func() {
						receiveMajority <- true
					})
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
					rf.votedFor = -1
				}
				rf.mu.Unlock()
			}
		}(i)
	}

	go rf.checkVotes(receiveMajority, electionTerm)
}

func (rf *Raft) checkVotes(receiveMajority chan bool, electionTerm int) {

	rf.mu.Lock()
	DPrintf("\t\t\t\t checkVotes id:%d, term: %d %q\n", rf.me, rf.currentTerm, rf.state)
	rf.mu.Unlock()

	select {
	case <-receiveMajority:
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Candidate || rf.currentTerm != electionTerm {
			return
		}
		rf.state = Leader
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
		// send heartbeat
		go rf.sendHeartbeat(rf.currentTerm)
	case <-time.After(time.Duration(400+(rand.Int63()%150)) * time.Millisecond):
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Candidate || rf.currentTerm != electionTerm {
			return
		}
		rf.currentTerm++
		go rf.startElection(rf.currentTerm)
	}
}

// The triggerHeartbeat() send heartbeat periodically if it is a leader
func (rf *Raft) triggerHeartbeat() {
	for rf.killed() == false {
		time.Sleep(rf.heartbeatTimeout)

		rf.mu.Lock()

		rf.Report()
		if rf.state == Leader {
			// send heartbeat
			go rf.sendHeartbeat(rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartbeat(heartBeatTerm int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if heartBeatTerm != rf.currentTerm {
		return
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(id int) {

			rf.mu.Lock()
			nextIndex := rf.nextIndex[id]
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = nextIndex - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = rf.log[args.PrevLogIndex+1:]
			args.LeaderCommit = rf.commitIndex
			// DPrintf("\t\t\t Heartbeat** %d %q to %d, prevLogIndex: %d, prevLogTerm: %d\n",
			// rf.me, rf.state, id, args.PrevLogIndex, args.PrevLogTerm)
			// DPrintf("\t\t\t\t %#v\n", args.Entries)
			// DPrintf("\t\t\t\t nextIndex: %#v matchIndex: %#v\n", rf.nextIndex, rf.matchIndex)
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(id, args, reply)
			if !ok {
				// DPrintf("\t\t\t send append entries false")
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Leader {
				return
			}

			if rf.currentTerm < reply.Term {
				rf.state = Follower
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				return
			}

			// State == Leader, CurrentTerm >= reply.Term
			//                              ^
			//                            not possible

			if reply.Success {
				rf.nextIndex[id] = nextIndex + len(args.Entries)
				rf.matchIndex[id] = rf.nextIndex[id] - 1

				savedCommitIndex := rf.commitIndex
				for i := rf.commitIndex + 1; i < len(rf.log); i++ {
					if rf.log[i].Term == rf.currentTerm {
						matchCount := 1
						for j := 0; j < len(rf.peers); j++ {
							if j == rf.me {
								continue
							}
							if rf.matchIndex[j] >= i {
								matchCount++
							}
						}
						if matchCount*2 > len(rf.peers) {
							rf.commitIndex = i
						}
					}
				}
				if rf.commitIndex != savedCommitIndex {
					// DPrintf("\t\t\t %d %q newCommitCh\n", rf.me, rf.state)
					rf.newCommitCh <- struct{}{}
				}
			} else {
				// optimized: reduce the number of rejected AppendEntries RPCs.
				// - The follower can include the term of the conflicting entry
				//   and the first index it stores for that term.
				// - The leader can decrement nextIndex to bypass all of the
				//   conflicting entries in that term.
				// one AppendEntries RPC will be required for each term with
				// conflicting entries, rather than one PRC per entry.
				if nextIndex > 1 {
					rf.nextIndex[id] = nextIndex - 1
				}
			}
		}(i)
	}
}

//
// Applier
// When commitIndex > lastApplied, apply log[lastApplied] to state machine.
// Once a follower learns that a log entry is committed, it applies the entry
// to its local state machine.
//
func (rf *Raft) applier() {
	for rf.killed() == false {
		for range rf.newCommitCh {
			rf.mu.Lock()
			// DPrintf("\t\t\trf.applier(), %d %q\n", rf.me, rf.state)

			savedLastApplied := rf.lastApplied
			var entries []LogEntry
			if rf.commitIndex > rf.lastApplied {
				entries = rf.log[rf.lastApplied+1 : rf.commitIndex+1]
				rf.lastApplied = rf.commitIndex
			}
			rf.mu.Unlock()
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					CommandIndex: savedLastApplied + i + 1,
					Command:      entry.Command,
				}
			}

		}
	}
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
	rf.applyCh = applyCh
	rf.newCommitCh = make(chan struct{}, 1)

	// Your initialization code here (2A, 2B, 2C)
	rf.state = Follower
	rf.heartbeatTimeout = 150 * time.Millisecond // must smaller than election timeout
	rf.lastHeartbeatTime = time.Unix(0, 0)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{
		{-1, nil},
	}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// send heartbeat package if it's a leader
	go rf.triggerHeartbeat()

	// start applier
	go rf.applier()

	DPrintf("Init Raft: %d %q\n", rf.me, rf.state)
	return rf
}
