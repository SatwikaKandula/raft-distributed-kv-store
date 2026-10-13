package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int

	Store map[string]string
}
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Store             map[string]string
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

// maybeCreateSnapshotLocked checks whether enough new entries
// have been applied since the previous snapshot.
//
// IMPORTANT:
// The caller must already hold n.mu.
func (n *Node) maybeCreateSnapshotLocked() error {

	// Number of applied entries that are not yet in a snapshot.
	entriesSinceSnapshot :=
		n.LastApplied - n.LastIncludedIndex

	if entriesSinceSnapshot < n.SnapshotThreshold {
		return nil
	}
	return n.createSnapshotLocked()
}

// createSnapshotLocked creates a snapshot through LastApplied
// and removes those old entries from n.Log.
//
// IMPORTANT:
// The caller must already hold n.mu.
func (n *Node) createSnapshotLocked() error {

	snapshotIndex := n.LastApplied

	// Nothing new to snapshot.
	if snapshotIndex <= n.LastIncludedIndex {
		return nil
	}

	// Find the term at the last entry going into the snapshot.
	snapshotTerm, exists :=
		n.termAtLocked(snapshotIndex)

	if !exists {
		return fmt.Errorf(
			"cannot find term for snapshot index %d",
			snapshotIndex,
		)
	}

	// Make a copy of the current state machine.
	storeCopy := make(
		map[string]string,
		len(n.Store),
	)

	for key, value := range n.Store {
		storeCopy[key] = value
	}

	snapshot := Snapshot{
		LastIncludedIndex: snapshotIndex,
		LastIncludedTerm:  snapshotTerm,
		Store:             storeCopy,
	}
	n.SnapshotStore = storeCopy
	data, err := json.MarshalIndent(
		snapshot,
		"",
		"  ",
	)
	if err != nil {
		return err
	}

	if err := os.MkdirAll("data", 0755); err != nil {
		return err
	}

	filename := filepath.Join(
		"data",
		fmt.Sprintf(
			"node%d_snapshot.json",
			n.ID,
		),
	)

	tempFile := filename + ".tmp"

	if err := os.WriteFile(
		tempFile,
		data,
		0644,
	); err != nil {
		return err
	}

	if err := os.Rename(
		tempFile,
		filename,
	); err != nil {
		return err
	}

	/*
		Now compact the in-memory log.

		Example:

		old LastIncludedIndex = -1
		snapshotIndex         = 9

		n.Log contains Raft indexes:

		    0 1 2 3 4 5 6 7 8 9 10 11

		We want to remove 0 through 9.

		removeCount =
		    9 - (-1)
		    = 10

		Remaining:

		    10 11
	*/
	removeCount :=
		snapshotIndex -
			n.LastIncludedIndex

	if removeCount > len(n.Log) {
		removeCount = len(n.Log)
	}

	remainingLog := make(
		[]LogEntry,
		len(n.Log[removeCount:]),
	)

	copy(
		remainingLog,
		n.Log[removeCount:],
	)

	n.Log = remainingLog

	// Update snapshot metadata.
	n.LastIncludedIndex = snapshotIndex
	n.LastIncludedTerm = snapshotTerm

	fmt.Printf(
		"Node %d created snapshot through index %d (term %d). Remaining log entries: %d\n",
		n.ID,
		n.LastIncludedIndex,
		n.LastIncludedTerm,
		len(n.Log),
	)

	// Persist the compacted Raft state too.
	return n.saveStateLocked()
}
func (n *Node) loadSnapshotLocked() error {
	filename := filepath.Join(
		"data",
		fmt.Sprintf("node%d_snapshot.json", n.ID),
	)

	data, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		if n.LastIncludedIndex >= 0 {
			return fmt.Errorf(
				"snapshot file missing for compacted state through index %d",
				n.LastIncludedIndex,
			)
		}
		return nil
	}
	if err != nil {
		return err
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	if snapshot.LastIncludedIndex != n.LastIncludedIndex ||
		snapshot.LastIncludedTerm != n.LastIncludedTerm {
		return fmt.Errorf(
			"snapshot metadata mismatch: state=(%d,%d) snapshot=(%d,%d)",
			n.LastIncludedIndex,
			n.LastIncludedTerm,
			snapshot.LastIncludedIndex,
			snapshot.LastIncludedTerm,
		)
	}

	n.SnapshotStore = make(map[string]string, len(snapshot.Store))
	n.Store = make(map[string]string, len(snapshot.Store))
	for key, value := range snapshot.Store {
		n.SnapshotStore[key] = value
		n.Store[key] = value
	}

	return nil
}

func (n *Node) InstallSnapshot(
	args InstallSnapshotArgs,
) InstallSnapshotReply {

	n.mu.Lock()
	defer n.mu.Unlock()

	reply := InstallSnapshotReply{
		Term:    n.CurrentTerm,
		Success: false,
	}

	// Simulated network partition.
	if n.BlockedPeers[args.LeaderID] {
		return reply
	}

	if !n.IsAlive {
		return reply
	}

	// Reject an old leader.
	if args.Term < n.CurrentTerm {
		return reply
	}

	termChanged := false
	if args.Term > n.CurrentTerm {
		n.CurrentTerm = args.Term
		n.VotedFor = -1
		termChanged = true
	}

	n.State = Follower
	n.LeaderID = args.LeaderID
	n.LastHeartbeat = time.Now()

	// We already have an equal or newer snapshot. Persist a newer term
	// before acknowledging so a crash cannot make us forget it.
	if args.LastIncludedIndex <= n.LastIncludedIndex {
		if termChanged {
			if err := n.saveStateLocked(); err != nil {
				return reply
			}
		}

		reply.Term = n.CurrentTerm
		reply.Success = true
		return reply
	}

	/*
		Check whether we can preserve log entries
		AFTER the incoming snapshot.
	*/
	oldLastIncludedIndex := n.LastIncludedIndex

	term, exists :=
		n.termAtLocked(args.LastIncludedIndex)

	if exists &&
		term == args.LastIncludedTerm {

		/*
			Example:

			old snapshot = -1
			new snapshot = 9

			remove first 10 entries,
			but preserve entries 10+.
		*/
		removeCount :=
			args.LastIncludedIndex -
				oldLastIncludedIndex

		if removeCount <= len(n.Log) {

			remaining := make(
				[]LogEntry,
				len(n.Log[removeCount:]),
			)

			copy(
				remaining,
				n.Log[removeCount:],
			)

			n.Log = remaining

		} else {
			n.Log = []LogEntry{}
		}

	} else {

		// Logs don't match snapshot boundary.
		n.Log = []LogEntry{}
	}

	// Install the snapshot state machine.
	n.Store = make(
		map[string]string,
		len(args.Store),
	)

	n.SnapshotStore = make(
		map[string]string,
		len(args.Store),
	)

	for key, value := range args.Store {

		n.Store[key] = value
		n.SnapshotStore[key] = value
	}

	n.LastIncludedIndex =
		args.LastIncludedIndex

	n.LastIncludedTerm =
		args.LastIncludedTerm

	// Preserve any already-known committed suffix that survived the snapshot
	// boundary. Rebuild Store from the snapshot and then re-apply that suffix.
	targetCommit := n.CommitIndex
	if targetCommit < args.LastIncludedIndex {
		targetCommit = args.LastIncludedIndex
	}
	lastLogIndex := n.lastLogIndexLocked()
	if targetCommit > lastLogIndex {
		targetCommit = lastLogIndex
	}

	n.CommitIndex = targetCommit
	n.LastApplied = args.LastIncludedIndex
	n.applyCommittedEntriesLocked()

	// Save snapshot file.
	snapshot := Snapshot{
		LastIncludedIndex: n.LastIncludedIndex,
		LastIncludedTerm:  n.LastIncludedTerm,
		Store:             n.SnapshotStore,
	}

	data, err := json.MarshalIndent(
		snapshot,
		"",
		"  ",
	)

	if err != nil {
		return reply
	}

	if err := os.MkdirAll(
		"data",
		0755,
	); err != nil {
		return reply
	}

	filename := filepath.Join(
		"data",
		fmt.Sprintf(
			"node%d_snapshot.json",
			n.ID,
		),
	)

	tempFile := filename + ".tmp"

	if err := os.WriteFile(
		tempFile,
		data,
		0644,
	); err != nil {
		return reply
	}

	if err := os.Rename(
		tempFile,
		filename,
	); err != nil {
		return reply
	}

	// Save compacted persistent Raft state.
	if err := n.saveStateLocked(); err != nil {
		return reply
	}

	fmt.Printf(
		"Node %d installed snapshot from Leader %d through index %d\n",
		n.ID,
		args.LeaderID,
		args.LastIncludedIndex,
	)

	reply.Term = n.CurrentTerm
	reply.Success = true

	return reply
}
