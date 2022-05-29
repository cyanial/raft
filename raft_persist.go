package raft

import (
	"bytes"

	"github.com/cyanial/raft/labgob"
)

type PersistState struct {
	CurrentTerm int
	VoteFor     int
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

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	persistState := PersistState{
		CurrentTerm: rf.currentTerm,
		VoteFor:     rf.votedFor,
		Log:         rf.log,
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
		DPrintf("rf.readPersist() error: %v\n", err)
		return
	}

	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.currentTerm = persistState.CurrentTerm
	rf.votedFor = persistState.VoteFor
	rf.log = persistState.Log

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
