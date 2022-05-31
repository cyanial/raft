package raft

import (
	"log"
)

// Debugging
var (
	debug bool = false
)

// Color for better debug
const colorReset string = "\033[0m"

var color = []string{
	"\033[31m",
	"\033[32m",
	"\033[33m",
	"\033[34m",
	"\033[35m",
	"\033[36m",
	"\033[37m",
	"\033[38m",
	"\033[39m",
	"\033[40m",
	"\033[41m",
	"\033[42m",
}

func init() {
	log.SetFlags(log.Ltime)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
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
