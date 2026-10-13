package raft

import "fmt"

func (n *Node) AppendNewCommand(command Command) (LogEntry, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	entry := LogEntry{
		Term:    n.CurrentTerm,
		Command: command,
	}
	n.Log = append(n.Log, entry)
	entryIndex := n.lastLogIndexLocked()
	//After modifing the log, we safe the new state to disk
	if err := n.saveStateLocked(); err != nil {
		fmt.Printf("Node %d failed to persist new log entry: %v\n", n.ID, err)
	}
	fmt.Printf("Leader Node %d appended command to its log at index %d: %s %s %s\n",
		n.ID,
		entryIndex,
		command.Type,
		command.Key,
		command.Value,
	)
	return entry, entryIndex
}
func (n *Node) PrintLog() {
	n.mu.Lock()
	defer n.mu.Unlock()
	fmt.Printf("Node %d Log:\n", n.ID)
	if len(n.Log) == 0 {
		fmt.Println(" empty")
		return
	}
	for i, entry := range n.Log {

		absoluteIndex :=
			n.LastIncludedIndex + 1 + i

		fmt.Printf(
			"  Index %d: Term %d, Command: %s %s %s\n",
			absoluteIndex,
			entry.Term,
			entry.Command.Type,
			entry.Command.Key,
			entry.Command.Value,
		)
	}
}
func (n *Node) CommitEntry(index int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// CommitIndex is monotonic in Raft. Never move it backward.
	if index <= n.CommitIndex {
		return
	}

	entry, exists := n.entryAtLocked(index)
	if !exists {
		return
	}

	n.CommitIndex = index
	cmd := entry.Command
	fmt.Printf(
		"Node %d committed log index %d: %s %s %s\n",
		n.ID,
		index,
		cmd.Type,
		cmd.Key,
		cmd.Value,
	)

	n.applyCommittedEntriesLocked()

	if err := n.maybeCreateSnapshotLocked(); err != nil {
		fmt.Printf(
			"Node %d failed to create snapshot: %v\n",
			n.ID,
			err,
		)
	}

	if err := n.saveStateLocked(); err != nil {
		fmt.Printf(
			"Node %d failed to persist committed state: %v\n",
			n.ID,
			err,
		)
	}
}

// advanceCommitIndexLocked applies Raft's leader-side majority rule.
// The caller must hold n.mu.
func (n *Node) advanceCommitIndexLocked() bool {
	if !n.IsAlive || n.State != Leader {
		return false
	}

	majority := (len(n.Peers)+1)/2 + 1
	lastIndex := n.lastLogIndexLocked()

	for index := lastIndex; index > n.CommitIndex; index-- {
		term, exists := n.termAtLocked(index)
		if !exists || term != n.CurrentTerm {
			continue
		}

		replicated := 1 // leader itself
		for _, peerID := range n.Peers {
			if n.MatchIndex[peerID] >= index {
				replicated++
			}
		}

		if replicated < majority {
			continue
		}

		n.CommitIndex = index
		n.applyCommittedEntriesLocked()

		if err := n.maybeCreateSnapshotLocked(); err != nil {
			fmt.Printf(
				"Node %d failed to create snapshot: %v\n",
				n.ID,
				err,
			)
		}

		if err := n.saveStateLocked(); err != nil {
			fmt.Printf(
				"Node %d failed to persist committed state: %v\n",
				n.ID,
				err,
			)
		}

		fmt.Printf(
			"Leader Node %d advanced CommitIndex to %d\n",
			n.ID,
			n.CommitIndex,
		)

		return true
	}

	return false
}

func (n *Node) ReplicateCommand(command Command) bool {
	// gRPC can invoke multiple writes concurrently. Serializing proposals
	// keeps log append/commit ordering simple and prevents an older RPC
	// from moving CommitIndex backward after a newer one commits.
	n.proposalMu.Lock()
	defer n.proposalMu.Unlock()

	n.mu.Lock()
	if !n.IsAlive || n.State != Leader {
		n.mu.Unlock()
		fmt.Printf(
			"Node %d is not leader. Cannot replicate command.\n",
			n.ID,
		)
		return false
	}

	entry := LogEntry{
		Term:    n.CurrentTerm,
		Command: command,
	}
	n.Log = append(n.Log, entry)
	entryIndex := n.lastLogIndexLocked()

	if err := n.saveStateLocked(); err != nil {
		n.mu.Unlock()
		fmt.Printf(
			"Node %d failed to persist new log entry: %v\n",
			n.ID,
			err,
		)
		return false
	}

	fmt.Printf(
		"Leader Node %d appended command to its log at index %d: %s %s %s\n",
		n.ID,
		entryIndex,
		command.Type,
		command.Key,
		command.Value,
	)
	n.mu.Unlock()

	for _, peerID := range n.Peers {
		n.ReplicateToPeer(peerID)

		n.mu.Lock()
		committed := n.CommitIndex >= entryIndex
		stillLeader := n.State == Leader && n.IsAlive
		n.mu.Unlock()

		if committed || !stillLeader {
			break
		}
	}

	n.mu.Lock()
	committed := n.CommitIndex >= entryIndex
	stillLeader := n.State == Leader && n.IsAlive
	n.mu.Unlock()

	if committed && stillLeader {
		// The next heartbeat carries the updated LeaderCommit to followers.
		return true
	}

	fmt.Println(
		"Command was not committed because majority was not reached",
	)
	return false
}

func (n *Node) ReplicateToPeer(peerID int) bool {

	// Simulated network partition.
	if n.IsPeerBlocked(peerID) {
		fmt.Printf(
			"Node %d -> Node %d blocked by simulated network partition\n",
			n.ID,
			peerID,
		)
		return false
	}

	n.mu.Lock()

	// Only a live leader can replicate logs.
	if !n.IsAlive || n.State != Leader {
		n.mu.Unlock()
		return false
	}

	// Find peer's gRPC address.
	address, exists := n.PeerAddresses[peerID]

	if !exists {
		n.mu.Unlock()

		fmt.Printf(
			"No address found for Node %d\n",
			peerID,
		)

		return false
	}

	n.mu.Unlock()

	// Retry until follower's log matches.
	for {

		n.mu.Lock()

		// Absolute Raft index of the next entry
		// this follower needs.
		nextIndex := n.NextIndex[peerID]

		// Absolute index of leader's last log entry.
		lastLogIndex := n.lastLogIndexLocked()

		// nextIndex should never be beyond
		// leader's last log entry + 1.
		if nextIndex > lastLogIndex+1 {
			nextIndex = lastLogIndex + 1
			n.NextIndex[peerID] = nextIndex
		}

		// This follower is so far behind that the entries
		// it needs have already been removed by snapshotting.
		//
		// Send InstallSnapshot when the follower is behind compacted history.
		if nextIndex <= n.LastIncludedIndex {

			snapshotStore := make(
				map[string]string,
				len(n.SnapshotStore),
			)

			for key, value := range n.SnapshotStore {
				snapshotStore[key] = value
			}

			snapshotArgs := InstallSnapshotArgs{
				Term:              n.CurrentTerm,
				LeaderID:          n.ID,
				LastIncludedIndex: n.LastIncludedIndex,
				LastIncludedTerm:  n.LastIncludedTerm,
				Store:             snapshotStore,
			}

			currentTerm := n.CurrentTerm

			n.mu.Unlock()

			fmt.Printf(
				"Leader Node %d sending snapshot to Node %d\n",
				n.ID,
				peerID,
			)

			reply, err :=
				SendInstallSnapshotRPC(
					address,
					snapshotArgs,
				)

			if err != nil {
				fmt.Printf(
					"Leader Node %d failed to send snapshot to Node %d: %v\n",
					n.ID,
					peerID,
					err,
				)

				return false
			}

			if reply.Term > currentTerm {
				n.BecomeFollower(reply.Term)
				return false
			}

			if !reply.Success {
				return false
			}

			n.mu.Lock()

			n.MatchIndex[peerID] =
				snapshotArgs.LastIncludedIndex

			n.NextIndex[peerID] =
				snapshotArgs.LastIncludedIndex + 1

			n.mu.Unlock()

			/*
				Don't return yet.

				Loop again because there may be newer
				log entries AFTER the snapshot.
			*/
			continue
		}

		// Entry immediately before the entries we're about to send.
		prevLogIndex := nextIndex - 1

		// Get the term using the absolute Raft index.
		prevLogTerm, exists := n.termAtLocked(
			prevLogIndex,
		)

		if !exists {
			fmt.Printf(
				"Leader Node %d could not find term for log index %d\n",
				n.ID,
				prevLogIndex,
			)

			n.mu.Unlock()
			return false
		}

		/*
			Convert absolute Raft index to Go slice index.

			Example:

			LastIncludedIndex = 9
			nextIndex         = 10

			sliceIndex =
				10 - 9 - 1
				= 0

			So:

			Raft index 10 -> n.Log[0]
		*/
		sliceIndex :=
			nextIndex -
				n.LastIncludedIndex -
				1

		// Send every entry the follower is missing.
		entriesToSend := make(
			[]LogEntry,
			len(n.Log[sliceIndex:]),
		)

		copy(
			entriesToSend,
			n.Log[sliceIndex:],
		)

		args := AppendEntriesArgs{
			Term:         n.CurrentTerm,
			LeaderID:     n.ID,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entriesToSend,
			LeaderCommit: n.CommitIndex,
		}

		currentTerm := n.CurrentTerm

		n.mu.Unlock()

		// Check partition again because it could have
		// been enabled while this function was running.
		if n.IsPeerBlocked(peerID) {
			fmt.Printf(
				"Node %d -> Node %d blocked by simulated network partition\n",
				n.ID,
				peerID,
			)
			return false
		}

		// Send AppendEntries through gRPC.
		reply, err := SendAppendEntriesRPC(
			address,
			args,
		)

		// RPC/network failure.
		if err != nil {
			fmt.Printf(
				"Leader Node %d could not reach Node %d: %v\n",
				n.ID,
				peerID,
				err,
			)

			return false
		}

		// Follower knows about a newer term.
		// This node must stop being leader.
		if reply.Term > currentTerm {
			n.BecomeFollower(reply.Term)
			return false
		}

		// Replication succeeded.
		if reply.Success {

			n.mu.Lock()

			/*
				Example:

				prevLogIndex = 9
				entries sent = 3

				matchIndex = 12

				So follower now definitely has
				entries through absolute index 12.
			*/
			matchIndex :=
				prevLogIndex +
					len(entriesToSend)

			n.MatchIndex[peerID] = matchIndex
			n.NextIndex[peerID] = matchIndex + 1

			n.advanceCommitIndexLocked()

			n.mu.Unlock()

			if len(entriesToSend) > 0 {
				fmt.Printf(
					"Leader Node %d replicated %d entries to Node %d\n",
					n.ID,
					len(entriesToSend),
					peerID,
				)
			}

			return true
		}

		/*
			Follower rejected AppendEntries because
			PrevLogIndex / PrevLogTerm didn't match.

			Move NextIndex backward and try again.
		*/
		n.mu.Lock()

		if n.NextIndex[peerID] > 0 {
			// If this leader has compacted its log, a rejection may mean the
			// follower is behind the snapshot boundary. Jump directly to the
			// snapshot instead of decrementing one index at a time.
			if n.LastIncludedIndex >= 0 && n.NextIndex[peerID] > n.LastIncludedIndex {
				n.NextIndex[peerID] = n.LastIncludedIndex
			} else {
				n.NextIndex[peerID]--
			}

			fmt.Printf(
				"Leader Node %d adjusted nextIndex for Node %d to %d\n",
				n.ID,
				peerID,
				n.NextIndex[peerID],
			)

			n.mu.Unlock()
			continue
		}

		n.mu.Unlock()

		return false
	}
}
