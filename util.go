package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
var debug bool = false

// Color for better debug
// const colorReset string = "\033[0m"

// var color = []string{
// 	"\033[31m",
// 	"\033[32m",
// 	"\033[33m",
// 	"\033[34m",
// 	"\033[35m",
// 	"\033[36m",
// 	"\033[37m",
// 	"\033[38m",
// 	"\033[39m",
// 	"\033[40m",
// 	"\033[41m",
// 	"\033[42m",
// }

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

// get a random election timeout
func randomElectionTime() time.Duration {
	return time.Duration((150 + (rand.Int63() % 150))) * (time.Millisecond)
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) getLastLogIndex() int {
	return rf.logSize() - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.getLastLog().Term
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

// check if candidate's log is at least as new as the voter.
func (rf *Raft) isUpToDate(candidateTerm int, candidateIndex int) bool {
	index, term := rf.getLastLogIndex(), rf.getLastLogTerm()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
}

// replace for len(rf.log)
func (rf *Raft) logSize() int {
	return rf.logBase + len(rf.log)
}

// replace for rf.log[i]
func (rf *Raft) logAt(index int) LogEntry {
	return rf.log[index-rf.logBase]
}

// replace for rf.log[i] = e
func (rf *Raft) logSetAt(index int, e LogEntry) {
	rf.log[index-rf.logBase] = e
}
