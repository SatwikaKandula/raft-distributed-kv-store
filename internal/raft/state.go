package raft

import (
	"fmt"
	"time"
)

func (n *Node) BecomeFollower(term int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.State = Follower
	n.LeaderID = -1
	n.LastHeartbeat = time.Now()
	if term > n.CurrentTerm {
		n.CurrentTerm = term
		n.VotedFor = -1
	}

	if err := n.saveStateLocked(); err != nil {
		fmt.Printf(
			"Node %d failed to persist follower state: %v\n",
			n.ID,
			err,
		)
	}

	fmt.Printf(
		"Node %d became Follower for term %d\n",
		n.ID,
		n.CurrentTerm,
	)
}
func (n *Node) BecomeCandidate() bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.IsAlive {
		return false
	}

	n.State = Candidate
	n.LeaderID = -1
	n.CurrentTerm++
	n.VotedFor = n.ID
	if err := n.saveStateLocked(); err != nil {
		fmt.Printf("Node %d failed to persist candidate state: %v\n", n.ID, err)
	}
	fmt.Printf("Node %d became Candidate in term %d\n", n.ID, n.CurrentTerm)
	return true
}
func (n *Node) BecomeLeader(term int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	// The election that collected the votes must still be current.
	if !n.IsAlive || n.State != Candidate || n.CurrentTerm != term {
		return false
	}

	n.State = Leader
	n.LeaderID = n.ID
	nextIndex := n.lastLogIndexLocked() + 1
	for _, peerID := range n.Peers {
		n.NextIndex[peerID] = nextIndex
		n.MatchIndex[peerID] = -1
	}
	fmt.Printf("Node %d became Leader for term %d\n", n.ID, n.CurrentTerm)
	return true
}
