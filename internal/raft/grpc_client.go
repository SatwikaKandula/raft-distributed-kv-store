package raft

import (
	"context"
	"time"

	raftpb "raft-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Wrapper for client side
func SendRequestVoteRPC(
	address string,
	args RequestVoteArgs,
) (RequestVoteReply, error) {
	//creates the connection to the remote node's address
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return RequestVoteReply{}, err
	}
	defer conn.Close()
	//Gives the generated client that can call RequestVote RPC
	client := raftpb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		750*time.Millisecond,
	)
	defer cancel()
	// converts noramal Go values into gRPC messages
	req := &raftpb.RequestVoteRequest{
		Term:         int32(args.Term),
		CandidateId:  int32(args.CandidateID),
		LastLogIndex: int32(args.LastLogIndex),
		LastLogTerm:  int32(args.LastLogTerm),
	}
	//sends the actual RPC
	response, err := client.RequestVote(ctx, req) // it sends the actual RPC

	if err != nil {
		return RequestVoteReply{}, err
	}

	return RequestVoteReply{
		Term:        int(response.Term),
		VoteGranted: response.VoteGranted,
	}, nil
}
func SendAppendEntriesRPC(
	address string,
	args AppendEntriesArgs,
) (AppendEntriesReply, error) {

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return AppendEntriesReply{}, err
	}

	defer conn.Close()

	client := raftpb.NewRaftServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		350*time.Millisecond,
	)
	defer cancel()

	pbEntries := make([]*raftpb.LogEntry, 0, len(args.Entries))

	for _, entry := range args.Entries {

		var pbCommandType raftpb.CommandType

		switch entry.Command.Type {

		case CommandNoOp:
			pbCommandType = raftpb.CommandType_COMMAND_TYPE_UNSPECIFIED

		case CommandPut:
			pbCommandType = raftpb.CommandType_PUT

		case CommandDelete:
			pbCommandType = raftpb.CommandType_DELETE

		default:
			continue
		}

		pbEntry := &raftpb.LogEntry{
			Term: int32(entry.Term),

			Command: &raftpb.Command{
				Type:  pbCommandType,
				Key:   entry.Command.Key,
				Value: entry.Command.Value,
			},
		}

		pbEntries = append(pbEntries, pbEntry)
	}

	req := &raftpb.AppendEntriesRequest{
		Term:         int32(args.Term),
		LeaderId:     int32(args.LeaderID),
		PrevLogIndex: int32(args.PrevLogIndex),
		PrevLogTerm:  int32(args.PrevLogTerm),
		Entries:      pbEntries,
		LeaderCommit: int32(args.LeaderCommit),
	}

	response, err := client.AppendEntries(ctx, req)

	if err != nil {
		return AppendEntriesReply{}, err
	}

	return AppendEntriesReply{
		Term:    int(response.Term),
		Success: response.Success,
	}, nil
}
func SendInstallSnapshotRPC(
	address string,
	args InstallSnapshotArgs,
) (InstallSnapshotReply, error) {

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		return InstallSnapshotReply{}, err
	}

	defer conn.Close()

	client :=
		raftpb.NewRaftServiceClient(conn)

	ctx, cancel :=
		context.WithTimeout(
			context.Background(),
			1*time.Second,
		)

	defer cancel()

	store := make(
		map[string]string,
		len(args.Store),
	)

	for key, value := range args.Store {
		store[key] = value
	}

	req := &raftpb.InstallSnapshotRequest{
		Term:              int32(args.Term),
		LeaderId:          int32(args.LeaderID),
		LastIncludedIndex: int32(args.LastIncludedIndex),
		LastIncludedTerm:  int32(args.LastIncludedTerm),
		Store:             store,
	}

	response, err :=
		client.InstallSnapshot(
			ctx,
			req,
		)

	if err != nil {
		return InstallSnapshotReply{}, err
	}

	return InstallSnapshotReply{
		Term:    int(response.Term),
		Success: response.Success,
	}, nil
}
