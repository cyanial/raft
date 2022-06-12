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
	logBase     int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) report() {
	for rf.killed() == false {

		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		DPrintf("%s[%d %9s], term:%2d,base:%d, comIdx:%3d, lasApp:%3d, votFor:%2d, log:%3d %d N,M:%v%v%s",
			color[rf.me],
			rf.me, rf.state, rf.currentTerm, rf.logBase, rf.commitIndex, rf.lastApplied, rf.votedFor,
			rf.logSize()-1, rf.getLastLogTerm(), rf.nextIndex, rf.matchIndex,
			colorReset)
		rf.mu.Unlock()

	}
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
	// defer rf.persist()

	// is not leader, return false
	if rf.state != Leader {
		return -1, -1, false
	}

	index := rf.logSize()
	term := rf.currentTerm

	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
	})

	// send heartbeat
	// rf.sendHeartbeat(term)

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
		wait_time := randomElectionTime()
		time.Sleep(wait_time)

		// heartbeat was received in the sleep period

		rf.mu.Lock()
		if !rf.lastHeartbeatTime.Add(wait_time).After(time.Now()) &&
			rf.state == Follower {
			// start election
			rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}

//
// start an election
//
func (rf *Raft) startElection(electionTerm int) {

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// if rf.state == Leader || rf.currentTerm != electionTerm {
	// 	return
	// }

	// defer rf.persist()

	rf.currentTerm++
	rf.state = Candidate

	mu_votes := sync.Mutex{}
	votes, voters := 1, len(rf.peers)
	rf.votedFor = rf.me

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	receiveMajority := make(chan struct{})
	once := sync.Once{}

	for i := 0; i < int(voters); i++ {
		if i == rf.me {
			continue
		}
		go func(id int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(id, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				// rf.persist()
				return
			}

			if rf.state != Candidate {
				return
			}

			if reply.VoteGranted {
				mu_votes.Lock()
				votes++
				if votes > voters/2 {
					once.Do(func() {
						receiveMajority <- struct{}{}
					})
				}
				mu_votes.Unlock()
			}
		}(i)
	}

	go rf.checkVotes(receiveMajority, rf.currentTerm)
}

func (rf *Raft) checkVotes(receiveMajority chan struct{}, electionTerm int) {

	select {

	case <-receiveMajority:
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != Candidate || rf.currentTerm != electionTerm {
			return
		}

		// defer rf.persist()

		rf.state = Leader
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.logSize()
			rf.matchIndex[i] = 0
		}
		// send heartbeat
		rf.sendHeartbeat(rf.currentTerm)

	case <-time.After(randomElectionTime()):
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != Candidate || rf.currentTerm != electionTerm {
			return
		}

		rf.startElection(rf.currentTerm)
	}
}

// The triggerHeartbeat() send heartbeat periodically if it is a leader
func (rf *Raft) triggerHeartbeat() {
	for rf.killed() == false {
		time.Sleep(rf.heartbeatTimeout)

		rf.mu.Lock()

		if rf.state == Leader {
			// send heartbeat
			rf.sendHeartbeat(rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartbeat(heartBeatTerm int) {

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// if rf.state != Leader || heartBeatTerm != rf.currentTerm {
	// 	return
	// }

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(id int) {

			rf.mu.Lock()
			if rf.state != Leader || heartBeatTerm != rf.currentTerm {
				rf.mu.Unlock()
				return
			}

			nextIndex := rf.nextIndex[id]

			if nextIndex <= rf.logBase {

				args := &InstallSnapshotArgs{
					Term:              heartBeatTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.logBase,
					LastIncludedTerm:  rf.log[0].Term,
					Data:              rf.persister.ReadSnapshot(),
				}
				rf.mu.Unlock()

				reply := &InstallSnapshotReply{}
				ok := rf.sendInstallSnapshot(id, args, reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Leader || heartBeatTerm != rf.currentTerm {
					return
				}

				// defer rf.persist()

				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}

				rf.nextIndex[id] = args.LastIncludedIndex + 1
				rf.matchIndex[id] = args.LastIncludedIndex

			} else {

				// nextIndex > rf.logBase
				// realIndex + rf.logBase = nextIndex

				args := &AppendEntriesArgs{
					Term:         heartBeatTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  rf.logAt(nextIndex - 1).Term,
					Entries:      make([]LogEntry, len(rf.log[nextIndex-rf.logBase:])),
					LeaderCommit: rf.commitIndex,
				}
				copy(args.Entries, rf.log[nextIndex-rf.logBase:])
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(id, args, reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Leader || heartBeatTerm != rf.currentTerm {
					return
				}

				// defer rf.persist()

				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}

				if reply.Success {
					rf.nextIndex[id] = nextIndex + len(args.Entries)
					rf.matchIndex[id] = rf.nextIndex[id] - 1

					savedCommitIndex := rf.commitIndex
					for i := rf.commitIndex + 1; i < rf.logSize(); i++ {
						if rf.logAt(i).Term == rf.currentTerm {
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
						rf.newCommitCh <- struct{}{}
					}
				} else {
					// - Upon receving a conflict response, the leader should first
					//   search its log for conflictTerm. If it finds an entry in its
					//   log with that term, it should set nextIndex to be the one
					//   beyond the index of the last entry in that term in its log
					// - If it does not find an entry with that term, it should set
					//   nextIndex = conflictIndex
					if reply.ConflictTerm == -1 {
						rf.nextIndex[id] = reply.ConflictIndex
					} else {
						found := false
						for i := rf.logSize() - 1; i >= rf.logBase; i-- {
							if rf.logAt(i).Term == reply.ConflictTerm {
								rf.nextIndex[id] = i + 1
								found = true
								break
							}
						}
						if found == false {
							rf.nextIndex[id] = reply.ConflictIndex
						}
					}
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
		select {
		case <-rf.newCommitCh:
			rf.mu.Lock()
			savedLastApplied := rf.lastApplied
			var entries []LogEntry
			if rf.commitIndex > rf.lastApplied {
				entries = append(entries, rf.log[rf.lastApplied+1-rf.logBase:rf.commitIndex+1-rf.logBase]...)
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
	rf.heartbeatTimeout = 100 * time.Millisecond // must smaller than election timeout
	rf.lastHeartbeatTime = time.Unix(0, 0)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{
		{-1, nil},
	}
	rf.logBase = 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// send heartbeat package if it's a leader
	go rf.triggerHeartbeat()

	// start applier
	go rf.applier()

	// reporter
	go rf.report()

	return rf
}
