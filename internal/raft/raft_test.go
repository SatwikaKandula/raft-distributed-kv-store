package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCandidateStepsDownOnSameTermAppendEntries(t *testing.T) {
	n := NewNode(1, []int{2, 3})
	n.State = Candidate
	n.CurrentTerm = 4
	n.VotedFor = 1

	reply := n.AppendEntries(AppendEntriesArgs{
		Term:         4,
		LeaderID:     2,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		LeaderCommit: -1,
	})

	if !reply.Success {
		t.Fatal("expected same-term AppendEntries to succeed")
	}
	if n.State != Follower {
		t.Fatalf("expected follower state, got %s", n.State)
	}
	if n.LeaderID != 2 {
		t.Fatalf("expected leader 2, got %d", n.LeaderID)
	}
}

func TestRequestVoteRejectsOutdatedLog(t *testing.T) {
	n := NewNode(2, []int{1, 3})
	n.CurrentTerm = 5
	n.Log = []LogEntry{{
		Term: 5,
		Command: Command{
			Type:  CommandPut,
			Key:   "x",
			Value: "1",
		},
	}}

	reply := n.RequestVote(RequestVoteArgs{
		Term:         5,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  4,
	})

	if reply.VoteGranted {
		t.Fatal("expected vote to be rejected for an outdated log")
	}
}

func TestFollowerDoesNotCommitPastItsLocalLog(t *testing.T) {
	withTempWorkingDir(t)

	n := NewNode(2, []int{1, 3})
	n.CurrentTerm = 1
	n.Log = []LogEntry{{
		Term: 1,
		Command: Command{
			Type:  CommandPut,
			Key:   "safe",
			Value: "yes",
		},
	}}

	reply := n.AppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  1,
		LeaderCommit: 10,
	})

	if !reply.Success {
		t.Fatal("expected heartbeat to succeed")
	}
	if n.CommitIndex != 0 {
		t.Fatalf("expected commit index 0, got %d", n.CommitIndex)
	}
	if n.Store["safe"] != "yes" {
		t.Fatal("expected the locally present committed entry to be applied")
	}
}

func TestCommitIndexNeverMovesBackward(t *testing.T) {
	withTempWorkingDir(t)

	n := NewNode(1, []int{2, 3})
	n.CurrentTerm = 1
	n.Log = []LogEntry{
		{Term: 1, Command: Command{Type: CommandPut, Key: "a", Value: "1"}},
		{Term: 1, Command: Command{Type: CommandPut, Key: "b", Value: "2"}},
	}

	n.CommitEntry(1)
	n.CommitEntry(0)

	if n.CommitIndex != 1 {
		t.Fatalf("commit index moved backward: %d", n.CommitIndex)
	}
	if n.Store["a"] != "1" || n.Store["b"] != "2" {
		t.Fatalf("unexpected state machine contents: %#v", n.Store)
	}
}

func TestSnapshotCompactsCommittedLog(t *testing.T) {
	withTempWorkingDir(t)

	n := NewNode(1, []int{2, 3})
	n.CurrentTerm = 2
	n.SnapshotThreshold = 2
	n.Log = []LogEntry{
		{Term: 2, Command: Command{Type: CommandPut, Key: "a", Value: "1"}},
		{Term: 2, Command: Command{Type: CommandPut, Key: "b", Value: "2"}},
	}

	n.CommitEntry(1)

	if n.LastIncludedIndex != 1 {
		t.Fatalf("expected snapshot through index 1, got %d", n.LastIncludedIndex)
	}
	if len(n.Log) != 0 {
		t.Fatalf("expected compacted in-memory log, got %d entries", len(n.Log))
	}
	if _, err := os.Stat(filepath.Join("data", "node1_snapshot.json")); err != nil {
		t.Fatalf("expected snapshot file: %v", err)
	}
}

func withTempWorkingDir(t *testing.T) {
	t.Helper()

	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	tempDir := t.TempDir()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = os.Chdir(oldDir)
	})
}

func TestRestartRebuildsStoreFromSnapshotAndLogSuffix(t *testing.T) {
	withTempWorkingDir(t)

	n := NewNode(1, []int{2, 3})
	n.CurrentTerm = 3
	n.SnapshotThreshold = 2
	n.Log = []LogEntry{
		{Term: 3, Command: Command{Type: CommandPut, Key: "a", Value: "1"}},
		{Term: 3, Command: Command{Type: CommandPut, Key: "b", Value: "2"}},
	}
	n.CommitEntry(1) // creates snapshot through index 1

	n.Log = append(n.Log, LogEntry{
		Term:    3,
		Command: Command{Type: CommandPut, Key: "c", Value: "3"},
	})
	n.CommitEntry(2) // remains as a log suffix after the snapshot

	restarted := NewNode(1, []int{2, 3})
	if err := restarted.LoadState(); err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}

	if restarted.Store["a"] != "1" ||
		restarted.Store["b"] != "2" ||
		restarted.Store["c"] != "3" {
		t.Fatalf("state machine was not reconstructed: %#v", restarted.Store)
	}
	if restarted.CommitIndex != 2 || restarted.LastApplied != 2 {
		t.Fatalf(
			"unexpected recovered indexes: commit=%d applied=%d",
			restarted.CommitIndex,
			restarted.LastApplied,
		)
	}
}
