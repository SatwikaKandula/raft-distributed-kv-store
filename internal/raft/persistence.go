package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type PersistentState struct {
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
	CommitIndex       int
	LastApplied       int
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (n *Node) saveStateLocked() error {
	// Persist Raft metadata and the uncompacted log suffix. The key-value
	// state itself is reconstructed from snapshot + committed log entries.
	state := PersistentState{
		CurrentTerm:       n.CurrentTerm,
		VotedFor:          n.VotedFor,
		Log:               n.Log,
		CommitIndex:       n.CommitIndex,
		LastApplied:       n.LastApplied,
		LastIncludedIndex: n.LastIncludedIndex,
		LastIncludedTerm:  n.LastIncludedTerm,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	if err := os.MkdirAll("data", 0755); err != nil {
		return err
	}

	filename := filepath.Join("data", fmt.Sprintf("node%d.json", n.ID))
	tempFile := filename + ".tmp"

	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return err
	}

	return os.Rename(tempFile, filename)
}

func (n *Node) LoadState() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	filename := filepath.Join("data", fmt.Sprintf("node%d.json", n.ID))
	data, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	state := PersistentState{
		VotedFor:          -1,
		CommitIndex:       -1,
		LastApplied:       -1,
		LastIncludedIndex: -1,
		LastIncludedTerm:  -1,
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	n.CurrentTerm = state.CurrentTerm
	n.VotedFor = state.VotedFor
	n.Log = state.Log
	if n.Log == nil {
		n.Log = []LogEntry{}
	}
	n.CommitIndex = state.CommitIndex
	n.LastIncludedIndex = state.LastIncludedIndex
	n.LastIncludedTerm = state.LastIncludedTerm

	// Rebuild the state machine rather than trusting a duplicated Store copy.
	n.Store = map[string]string{}
	n.SnapshotStore = map[string]string{}

	if n.LastIncludedIndex >= 0 {
		if err := n.loadSnapshotLocked(); err != nil {
			return err
		}
	}

	if n.CommitIndex < n.LastIncludedIndex {
		n.CommitIndex = n.LastIncludedIndex
	}
	lastLogIndex := n.lastLogIndexLocked()
	if n.CommitIndex > lastLogIndex {
		n.CommitIndex = lastLogIndex
	}

	n.LastApplied = n.LastIncludedIndex
	n.applyCommittedEntriesLocked()

	// Volatile state always restarts as a follower with no known leader.
	n.State = Follower
	n.LeaderID = -1

	fmt.Printf(
		"Node %d restored persistent state: %d in-memory log entries, commit index %d\n",
		n.ID,
		len(n.Log),
		n.CommitIndex,
	)
	return nil
}
