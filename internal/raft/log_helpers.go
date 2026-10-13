package raft

// Returns the absolute Raft index of the last log entry.
func (n *Node) lastLogIndexLocked() int {
	return n.LastIncludedIndex + len(n.Log)
}

// Returns the term at an absolute Raft log index.
func (n *Node) termAtLocked(index int) (int, bool) {
	// Beginning of log.
	if index == -1 {
		return -1, true
	}
	// This entry is represented by the snapshot.
	if index == n.LastIncludedIndex {
		return n.LastIncludedTerm, true
	}
	// Entry is older than our snapshot.
	if index < n.LastIncludedIndex {
		return 0, false
	}
	// Convert absolute Raft index → slice index.
	sliceIndex := index - n.LastIncludedIndex - 1
	if sliceIndex < 0 || sliceIndex >= len(n.Log) {
		return 0, false
	}
	return n.Log[sliceIndex].Term, true
}

// Returns a log entry using its absolute Raft index.
func (n *Node) entryAtLocked(index int) (LogEntry, bool) {
	if index <= n.LastIncludedIndex {
		return LogEntry{}, false
	}
	sliceIndex := index - n.LastIncludedIndex - 1
	if sliceIndex < 0 || sliceIndex >= len(n.Log) {
		return LogEntry{}, false
	}
	return n.Log[sliceIndex], true
}
