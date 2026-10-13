package raft

import (
	"fmt"
	"sync"
	"time"
)

type NodeState string

const (
	Follower  NodeState = "Follower"
	Candidate NodeState = "Candidate"
	Leader    NodeState = "Leader"
)

type LogEntry struct {
	Term    int
	Command Command
}
type Node struct {
	//A structure defination creating a new data structure
	mu                sync.Mutex
	proposalMu        sync.Mutex
	ID                int
	State             NodeState
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
	CommitIndex       int
	LastApplied       int
	Peers             []int
	PeerAddresses     map[int]string
	Store             map[string]string
	IsAlive           bool
	NextIndex         map[int]int
	MatchIndex        map[int]int
	LastHeartbeat     time.Time
	LeaderID          int
	BlockedPeers      map[int]bool
	LastIncludedIndex int
	LastIncludedTerm  int
	SnapshotThreshold int
	SnapshotStore     map[string]string
}

func NewNode(id int, peers []int) *Node {
	return &Node{
		ID:                id,
		State:             Follower,
		CurrentTerm:       0,
		VotedFor:          -1,
		Log:               []LogEntry{},
		CommitIndex:       -1,
		LastApplied:       -1,
		Peers:             peers,
		PeerAddresses:     map[int]string{},
		Store:             map[string]string{},
		IsAlive:           true,
		BlockedPeers:      map[int]bool{},
		NextIndex:         map[int]int{},
		MatchIndex:        map[int]int{},
		LeaderID:          -1,
		LastHeartbeat:     time.Now(),
		LastIncludedIndex: -1,
		LastIncludedTerm:  -1,
		SnapshotThreshold: 10,
		SnapshotStore:     map[string]string{},
	}
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.IsAlive && n.State == Leader
}

func (n *Node) CurrentLeaderAddress() string {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.LeaderID == -1 || n.LeaderID == n.ID {
		return ""
	}

	return n.PeerAddresses[n.LeaderID]
}

func (n *Node) SetPeerAddress(peerID int, address string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.PeerAddresses[peerID] = address
}
func (n *Node) BlockPeer(peerID int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.BlockedPeers[peerID] = true

	fmt.Printf("Node %d blocked Node %d\n", n.ID, peerID)
}

func (n *Node) UnblockPeer(peerID int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.BlockedPeers, peerID)

	fmt.Printf(
		"Node %d unblocked Node %d\n",
		n.ID,
		peerID,
	)
}

func (n *Node) IsPeerBlocked(peerID int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.BlockedPeers[peerID]
}
