package raft

import "log"

// Debugging
var Debug bool = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getLastLogIndex()].Term
}

func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	return rf.getLastLogTerm(), rf.getLastLogIndex()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

// check if candidate's log is at least as new as the voter.
func (rf *Raft) isUpToDate(candidateTerm int, candidateIndex int) bool {
	term, index := rf.getLastLogTermAndIndex()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
}
