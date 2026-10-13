package raft

import "fmt"

func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.IsAlive = false
	fmt.Printf("Node %d stopped\n", n.ID)
}
func (n *Node) Restart() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.IsAlive = true
	fmt.Printf("Node %d restarted\n", n.ID)
}
