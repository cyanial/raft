package raft

import (
	"bytes"

	"github.com/cyanial/raft/labgob"
)

type PersistState struct {
	CurrentTerm int
	VoteFor     int
	LogBase     int
	Log         []LogEntry
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see papaer's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// the caller hold the mutex locked

	persistState := PersistState{
		CurrentTerm: rf.currentTerm,
		VoteFor:     rf.votedFor,
		Log:         rf.log,
		LogBase:     rf.logBase,
	}
	err := e.Encode(persistState)
	if err != nil {
		DPrintf("rf.persisit() encode error: %v\n", err)
		return
	}
	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var persistState PersistState
	err := d.Decode(&persistState)
	if err != nil {
		return
	}

	// the caller hold the mutex locked

	rf.currentTerm = persistState.CurrentTerm
	rf.votedFor = persistState.VoteFor
	rf.log = persistState.Log
	rf.logBase = persistState.LogBase
	rf.commitIndex = persistState.LogBase
	rf.lastApplied = persistState.LogBase
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// the caller hold the mutex locked

	persistState := PersistState{
		CurrentTerm: rf.currentTerm,
		VoteFor:     rf.votedFor,
		Log:         rf.log,
		LogBase:     rf.logBase,
	}
	err := e.Encode(persistState)
	if err != nil {
		return
	}
	rf.persister.SaveStateAndSnapshot(w.Bytes(), snapshot)
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}
