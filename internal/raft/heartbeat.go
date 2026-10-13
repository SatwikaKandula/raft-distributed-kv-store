package raft

import (
	"fmt"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (n *Node) StartHeartbeat(interval time.Duration) {
	//This is go rountine
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			n.mu.Lock()
			alive := n.IsAlive
			isLeader := n.State == Leader
			n.mu.Unlock()

			if !alive || !isLeader {
				fmt.Printf("Node %d stopped sending heartbeats.\n", n.ID)
				return
			}
			fmt.Printf("\nLeader Node %d sending heartbeat...\n", n.ID)
			for _, peerID := range n.Peers {
				n.ReplicateToPeer(peerID)
			}
		}
	}()
}
func (n *Node) SendCommitUpdate() {
	for _, peerID := range n.Peers {
		n.ReplicateToPeer(peerID)
	}
}
func (n *Node) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {

	n.mu.Lock()
	defer n.mu.Unlock()

	// Simulated network partition.
	if n.BlockedPeers[args.LeaderID] {
		fmt.Printf(
			"Node %d rejected AppendEntries from Node %d: network partition\n",
			n.ID,
			args.LeaderID,
		)

		return AppendEntriesReply{
			Term:    n.CurrentTerm,
			Success: false,
		}
	}

	// Dead node cannot process AppendEntries.
	if !n.IsAlive {
		fmt.Printf(
			"Node %d is down. AppendEntries ignored.\n",
			n.ID,
		)

		return AppendEntriesReply{
			Term:    n.CurrentTerm,
			Success: false,
		}
	}

	reply := AppendEntriesReply{
		Term:    n.CurrentTerm,
		Success: false,
	}

	// Leader is from an older term.
	if args.Term < n.CurrentTerm {
		fmt.Printf(
			"Node %d rejected AppendEntries from Node %d: leader term is old\n",
			n.ID,
			args.LeaderID,
		)

		return reply
	}

	// A valid AppendEntries for our current or a newer term means
	// this node must behave as a follower. A candidate can receive
	// AppendEntries from the winner of the same-term election.
	termChanged := false
	if args.Term > n.CurrentTerm {
		n.CurrentTerm = args.Term
		n.VotedFor = -1
		termChanged = true
	}
	if n.State != Follower {
		n.State = Follower
	}

	reply.Term = n.CurrentTerm

	// CurrentTerm and VotedFor are persistent Raft state.
	if termChanged {
		if err := n.saveStateLocked(); err != nil {
			fmt.Printf(
				"Node %d failed to persist new term: %v\n",
				n.ID,
				err,
			)

			return reply
		}
	}

	// We heard from a valid leader.
	n.LastHeartbeat = time.Now()
	n.LeaderID = args.LeaderID

	/*
		Check whether PrevLogIndex / PrevLogTerm match.

		This is now snapshot-safe.

		We cannot do:

		    n.Log[args.PrevLogIndex]

		anymore because after snapshotting:

		    Raft index 10 might be n.Log[0]
	*/
	prevLogTerm, exists := n.termAtLocked(
		args.PrevLogIndex,
	)

	if !exists {
		fmt.Printf(
			"Node %d rejected AppendEntries: missing PrevLogIndex %d\n",
			n.ID,
			args.PrevLogIndex,
		)

		return reply
	}

	if prevLogTerm != args.PrevLogTerm {
		fmt.Printf(
			"Node %d rejected AppendEntries: term mismatch at PrevLogIndex %d\n",
			n.ID,
			args.PrevLogIndex,
		)

		return reply
	}

	// Track whether persistent state actually changed.
	stateChanged := false

	/*
		The first incoming entry belongs immediately after
		PrevLogIndex.
	*/
	insertIndex := args.PrevLogIndex + 1

	for i, entry := range args.Entries {

		// This is the REAL Raft index.
		absoluteIndex := insertIndex + i

		/*
			If this entry is already represented by our snapshot,
			we don't need to store it again.

			Example:

			    LastIncludedIndex = 9

			Everything <= 9 is already in snapshot.
		*/
		if absoluteIndex <= n.LastIncludedIndex {
			continue
		}

		/*
			Convert absolute Raft index → Go slice index.

			Example:

			    LastIncludedIndex = 9
			    absoluteIndex     = 10

			    sliceIndex = 10 - 9 - 1
			               = 0

			So Raft index 10 is stored at n.Log[0].
		*/
		sliceIndex :=
			absoluteIndex -
				n.LastIncludedIndex -
				1

		if sliceIndex < len(n.Log) {

			// Entry already exists at this position.
			if n.Log[sliceIndex].Term != entry.Term {

				/*
					Conflict found.

					Remove this conflicting entry and
					everything after it.
				*/
				n.Log = n.Log[:sliceIndex]

				/*
					Append this entry and all remaining
					entries from the leader.
				*/
				n.Log = append(
					n.Log,
					args.Entries[i:]...,
				)

				stateChanged = true

				break
			}

			// Same term → existing entry matches.
			continue
		}

		if sliceIndex == len(n.Log) {

			// We reached the end of follower's log.
			n.Log = append(
				n.Log,
				args.Entries[i:]...,
			)

			stateChanged = true

			break
		}

		/*
			This would mean there is a gap in the follower's log.

			Normally this should not happen because PrevLogIndex
			was already verified.
		*/
		fmt.Printf(
			"Node %d rejected AppendEntries: log gap at index %d\n",
			n.ID,
			absoluteIndex,
		)

		return reply
	}

	/*
		Update follower CommitIndex.

		DO NOT use:

		    len(n.Log) - 1

		because after snapshotting that is only a slice index.

		lastLogIndexLocked() returns the absolute Raft index.
	*/
	if args.LeaderCommit > n.CommitIndex {

		lastNewIndex := n.lastLogIndexLocked()

		newCommitIndex := args.LeaderCommit

		if newCommitIndex > lastNewIndex {
			newCommitIndex = lastNewIndex
		}

		if newCommitIndex > n.CommitIndex {

			n.CommitIndex = newCommitIndex

			// Apply newly committed commands to the KV store.
			n.applyCommittedEntriesLocked()

			// Check whether this follower has enough
			// committed entries to create a snapshot.
			if err := n.maybeCreateSnapshotLocked(); err != nil {
				fmt.Printf(
					"Node %d failed to create snapshot: %v\n",
					n.ID,
					err,
				)
			}
			stateChanged = true

			fmt.Printf(
				"Node %d updated CommitIndex to %d\n",
				n.ID,
				n.CommitIndex,
			)
		}
	}

	/*
		Persist before telling the leader that AppendEntries
		succeeded.

		We only save when something actually changed, so normal
		heartbeats don't rewrite nodeX.json every 500 ms.
	*/
	if stateChanged {

		if err := n.saveStateLocked(); err != nil {
			fmt.Printf(
				"Node %d failed to persist AppendEntries state: %v\n",
				n.ID,
				err,
			)

			return reply
		}
	}

	reply.Term = n.CurrentTerm
	reply.Success = true

	if len(args.Entries) == 0 {
		fmt.Printf(
			"Node %d accepted heartbeat from Leader %d\n",
			n.ID,
			args.LeaderID,
		)
	} else {
		fmt.Printf(
			"Node %d accepted AppendEntries from Leader %d. Log entries remaining in memory: %d\n",
			n.ID,
			args.LeaderID,
			len(n.Log),
		)
	}

	return reply
}
