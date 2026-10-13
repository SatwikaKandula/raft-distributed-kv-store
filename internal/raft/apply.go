package raft

import "fmt"

func (n *Node) ApplyCommittedEntries() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applyCommittedEntriesLocked()
}

func (n *Node) applyCommittedEntriesLocked() {
	for n.LastApplied < n.CommitIndex {
		nextIndex := n.LastApplied + 1

		// Entries through LastIncludedIndex are already represented by snapshot.
		if nextIndex <= n.LastIncludedIndex {
			n.LastApplied = nextIndex
			continue
		}

		entry, exists := n.entryAtLocked(nextIndex)
		if !exists {
			fmt.Printf(
				"Node %d cannot apply committed index %d: log entry is missing\n",
				n.ID,
				nextIndex,
			)
			return
		}

		switch entry.Command.Type {
		case CommandNoOp:
			// Leadership barrier entry. It changes no user-visible state.
		case CommandPut:
			n.Store[entry.Command.Key] = entry.Command.Value
		case CommandDelete:
			delete(n.Store, entry.Command.Key)
		}

		n.LastApplied = nextIndex

		fmt.Printf(
			"Node %d applied log index %d: %s %s %s\n",
			n.ID,
			n.LastApplied,
			entry.Command.Type,
			entry.Command.Key,
			entry.Command.Value,
		)
	}
}

func (n *Node) PrintStore() {
	n.mu.Lock()
	defer n.mu.Unlock()
	fmt.Printf("Node %d Store:\n", n.ID)
	if len(n.Store) == 0 {
		fmt.Println("  empty")
		return
	}
	for key, value := range n.Store {
		fmt.Printf("  %s = %s\n", key, value)
	}
}
