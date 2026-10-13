package raft

import "fmt"

func (n *Node) GetValue(key string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	value, exists := n.Store[key]
	if exists {
		fmt.Printf("Node %d GET %s = %s\n", n.ID, key, value)
	} else {
		fmt.Printf("Node %d GET %s: key not found\n", n.ID, key)
	}
	return value, exists
}

// ConfirmLeadership exchanges Raft traffic with a majority before a read.
// Together with the current-term no-op committed on election, this prevents an
// isolated old leader from serving stale reads during a partition.
func (n *Node) ConfirmLeadership() bool {
	if !n.IsLeader() {
		return false
	}

	majority := (len(n.Peers)+1)/2 + 1
	acknowledgements := 1 // the leader itself

	for _, peerID := range n.Peers {
		if n.ReplicateToPeer(peerID) {
			acknowledgements++
		}

		if !n.IsLeader() {
			return false
		}

		if acknowledgements >= majority {
			return n.hasCommittedCurrentTermEntry()
		}
	}

	return false
}

func (n *Node) hasCommittedCurrentTermEntry() bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.CommitIndex < 0 {
		return false
	}

	term, exists := n.termAtLocked(n.CommitIndex)
	return exists && term == n.CurrentTerm
}
