package raft

import (
	"fmt"
	"math/rand"
	"time"
)

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// --------------------------------------------------
// Handle vote request from a candidate
// --------------------------------------------------
func (n *Node) RequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.BlockedPeers[args.CandidateID] {
		fmt.Printf(
			"Node %d rejected RequestVote from Node %d: network partition\n",
			n.ID,
			args.CandidateID,
		)

		return RequestVoteReply{
			Term:        n.CurrentTerm,
			VoteGranted: false,
		}
	}
	if !n.IsAlive {
		fmt.Printf("Node %d is down. Vote request ignored.\n", n.ID)

		return RequestVoteReply{
			Term:        n.CurrentTerm,
			VoteGranted: false,
		}
	}

	reply := RequestVoteReply{
		Term:        n.CurrentTerm,
		VoteGranted: false,
	}

	// 1. Reject candidate with an older term.
	if args.Term < n.CurrentTerm {
		fmt.Printf(
			"Node %d rejected vote for Node %d: candidate term is old\n",
			n.ID,
			args.CandidateID,
		)

		return reply
	}

	// 2. Candidate has a newer term.
	if args.Term > n.CurrentTerm {
		n.State = Follower
		n.LeaderID = -1
		n.CurrentTerm = args.Term
		n.VotedFor = -1
		reply.Term = n.CurrentTerm
		if err := n.saveStateLocked(); err != nil {
			fmt.Printf(
				"Node %d failed to persist term update: %v\n",
				n.ID,
				err,
			)
		}
	}

	// -----------------------------------------
	// 3. Check whether candidate's log is
	//    at least as up-to-date as my log.
	// -----------------------------------------

	myLastLogIndex := n.lastLogIndexLocked()
	myLastLogTerm, exists := n.termAtLocked(myLastLogIndex)
	if !exists {
		myLastLogTerm = -1
	}
	candidateLogUpToDate := false
	if args.LastLogTerm > myLastLogTerm {
		candidateLogUpToDate = true

	} else if args.LastLogTerm == myLastLogTerm &&
		args.LastLogIndex >= myLastLogIndex {

		candidateLogUpToDate = true
	}

	if !candidateLogUpToDate {
		fmt.Printf(
			"Node %d rejected vote for Node %d: candidate log is outdated\n",
			n.ID,
			args.CandidateID,
		)

		return reply
	}
	// -----------------------------------------
	// 4. Vote if we haven't voted yet,
	//    or already voted for this candidate.
	// -----------------------------------------
	if n.VotedFor == -1 || n.VotedFor == args.CandidateID {
		n.VotedFor = args.CandidateID
		if err := n.saveStateLocked(); err != nil {
			fmt.Printf(
				"Node %d failed to persist vote: %v\n",
				n.ID,
				err,
			)
		}
		reply.VoteGranted = true
		reply.Term = n.CurrentTerm
		n.LastHeartbeat = time.Now()
		fmt.Printf(
			"Node %d voted for Node %d in term %d\n",
			n.ID,
			args.CandidateID,
			n.CurrentTerm,
		)
		return reply
	}
	// 5. Already voted for somebody else.
	fmt.Printf(
		"Node %d rejected vote for Node %d: already voted for Node %d\n",
		n.ID,
		args.CandidateID,
		n.VotedFor,
	)
	return reply
}

// --------------------------------------------------
// Start a new election
// --------------------------------------------------
func (n *Node) StartElection() {
	n.mu.Lock()
	if !n.IsAlive {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	// BecomeCandidate increments the term and votes for itself.
	if !n.BecomeCandidate() {
		return
	}

	n.mu.Lock()
	term := n.CurrentTerm
	lastLogIndex := n.lastLogIndexLocked()
	lastLogTerm, exists := n.termAtLocked(lastLogIndex)
	if !exists {
		lastLogTerm = -1
	}
	n.LastHeartbeat = time.Now()
	n.mu.Unlock()

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	type voteResult struct {
		peerID int
		reply  RequestVoteReply
		err    error
	}

	results := make(chan voteResult, len(n.Peers))
	requests := 0

	// Request votes concurrently so an unavailable peer does not delay
	// the entire election.
	for _, peerID := range n.Peers {
		if n.IsPeerBlocked(peerID) {
			continue
		}

		n.mu.Lock()
		address, ok := n.PeerAddresses[peerID]
		n.mu.Unlock()
		if !ok {
			continue
		}

		requests++
		go func(id int, addr string) {
			reply, err := SendRequestVoteRPC(addr, args)
			results <- voteResult{peerID: id, reply: reply, err: err}
		}(peerID, address)
	}

	majority := (len(n.Peers)+1)/2 + 1
	votes := 1
	received := 0
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()

	for received < requests {
		select {
		case result := <-results:
			received++

			if result.err != nil {
				fmt.Printf(
					"Node %d could not request vote from Node %d: %v\n",
					n.ID,
					result.peerID,
					result.err,
				)
				continue
			}

			if result.reply.Term > term {
				n.BecomeFollower(result.reply.Term)
				return
			}

			if result.reply.VoteGranted {
				votes++
			}

			if votes >= majority {
				fmt.Printf(
					"Node %d received %d votes. Majority needed: %d\n",
					n.ID,
					votes,
					majority,
				)

				if !n.BecomeLeader(term) {
					return
				}

				// Commit a current-term no-op entry. Besides establishing leadership,
				// this also safely commits any older entries that are already on a
				// majority before linearizable reads are served.
				n.ReplicateCommand(Command{Type: CommandNoOp})
				n.StartHeartbeat(300 * time.Millisecond)
				return
			}

		case <-timer.C:
			fmt.Printf(
				"Node %d election term %d timed out with %d votes\n",
				n.ID,
				term,
				votes,
			)
			return
		}
	}

	fmt.Printf(
		"Node %d received %d votes. Majority needed: %d\n",
		n.ID,
		votes,
		majority,
	)
}

// --------------------------------------------------
// Monitor leader heartbeat
// --------------------------------------------------
func randomElectionTimeout() time.Duration {
	return time.Duration(1200+rand.Intn(1000)) * time.Millisecond
}

func (n *Node) StartElectionTimer() {
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		timeout := randomElectionTimeout()
		var observedHeartbeat time.Time

		for range ticker.C {
			n.mu.Lock()

			if !n.IsAlive || n.State == Leader {
				n.mu.Unlock()
				continue
			}

			lastHeartbeat := n.LastHeartbeat
			if lastHeartbeat.After(observedHeartbeat) {
				observedHeartbeat = lastHeartbeat
				timeout = randomElectionTimeout()
			}

			timedOut := time.Since(lastHeartbeat) >= timeout
			n.mu.Unlock()

			if !timedOut {
				continue
			}

			fmt.Printf(
				"Node %d election timeout: no heartbeat received\n",
				n.ID,
			)

			n.StartElection()
		}
	}()
}
