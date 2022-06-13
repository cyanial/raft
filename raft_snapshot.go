package raft

//
// A service wants to switch to snapshot. Only do so if Raft hasn't
// have more recent info since it commnunicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludeTerm int, lastIncludeIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// Previously, this lab recommended that you implement a funtion called
	// CondInstallSnapshot to avoid the requirement that snapshots and log
	// entries sent on applyCh are coordinated. This vestigal API interface
	// remains, but you are discouraged from implementing it; instead, we
	// suggest that you simply have it return true.

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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.logBase {
		// have a snapshot
		return
	}

	// discard the log before index, and set logBase equal
	// to index.
	rf.log = append([]LogEntry(nil), rf.log[index-rf.logBase:]...)
	rf.logBase = index

	rf.persistStateAndSnapshot(snapshot)
}
