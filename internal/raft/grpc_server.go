package raft

//This is a wrapper for server side
import (
	"context"
	raftpb "raft-kv-store/proto"
)

type GRPCServer struct {
	raftpb.UnimplementedRaftServiceServer

	Node *Node
}

func (s *GRPCServer) RequestVote(
	ctx context.Context,
	req *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteResponse, error) {
	//chaning to the normal GO values
	args := RequestVoteArgs{
		Term:         int(req.Term),
		CandidateID:  int(req.CandidateId),
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	}
	//calls the Raft vote logic in election.go
	//s.Node gives like the node id..
	reply := s.Node.RequestVote(args)
	return &raftpb.RequestVoteResponse{
		Term:        int32(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}
func (s *GRPCServer) AppendEntries(
	ctx context.Context,
	req *raftpb.AppendEntriesRequest,
) (*raftpb.AppendEntriesResponse, error) {

	entries := make([]LogEntry, 0, len(req.Entries))

	for _, pbEntry := range req.Entries {

		if pbEntry == nil || pbEntry.Command == nil {
			continue
		}

		var commandType CommandType

		switch pbEntry.Command.Type {

		case raftpb.CommandType_COMMAND_TYPE_UNSPECIFIED:
			commandType = CommandNoOp

		case raftpb.CommandType_PUT:
			commandType = CommandPut

		case raftpb.CommandType_DELETE:
			commandType = CommandDelete

		default:
			continue
		}

		entry := LogEntry{
			Term: int(pbEntry.Term),

			Command: Command{
				Type:  commandType,
				Key:   pbEntry.Command.Key,
				Value: pbEntry.Command.Value,
			},
		}

		entries = append(entries, entry)
	}

	args := AppendEntriesArgs{
		Term:         int(req.Term),
		LeaderID:     int(req.LeaderId),
		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int(req.LeaderCommit),
	}

	reply := s.Node.AppendEntries(args)

	return &raftpb.AppendEntriesResponse{
		Term:    int32(reply.Term),
		Success: reply.Success,
	}, nil
}
