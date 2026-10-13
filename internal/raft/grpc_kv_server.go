package raft

import (
	"context"

	raftpb "raft-kv-store/proto"
)

type KVGRPCServer struct {
	raftpb.UnimplementedKVServiceServer

	Node *Node
}

func (s *KVGRPCServer) getLeaderAddress() string {
	return s.Node.CurrentLeaderAddress()
}

func (s *KVGRPCServer) Put(
	ctx context.Context,
	req *raftpb.PutRequest,
) (*raftpb.PutResponse, error) {

	if !s.Node.IsLeader() {
		return &raftpb.PutResponse{
			Success:       false,
			Message:       "This node is not the leader",
			LeaderAddress: s.getLeaderAddress(),
		}, nil
	}

	command := Command{
		Type:  CommandPut,
		Key:   req.Key,
		Value: req.Value,
	}

	success := s.Node.ReplicateCommand(command)

	if !success {
		return &raftpb.PutResponse{
			Success: false,
			Message: "Failed to reach Raft majority",
		}, nil
	}

	return &raftpb.PutResponse{
		Success: true,
		Message: "PUT committed successfully",
	}, nil
}

func (s *KVGRPCServer) Get(
	ctx context.Context,
	req *raftpb.GetRequest,
) (*raftpb.GetResponse, error) {

	if !s.Node.IsLeader() {
		return &raftpb.GetResponse{
			Success:       false,
			Message:       "This node is not the leader",
			LeaderAddress: s.getLeaderAddress(),
		}, nil
	}

	if !s.Node.ConfirmLeadership() {
		return &raftpb.GetResponse{
			Success: false,
			Message: "Leader could not confirm a Raft majority; read rejected",
		}, nil
	}

	s.Node.mu.Lock()
	value, exists := s.Node.Store[req.Key]
	s.Node.mu.Unlock()

	if !exists {
		return &raftpb.GetResponse{
			Success: false,
			Message: "Key not found",
		}, nil
	}

	return &raftpb.GetResponse{
		Success: true,
		Value:   value,
		Message: "GET successful",
	}, nil
}

func (s *KVGRPCServer) Delete(
	ctx context.Context,
	req *raftpb.DeleteRequest,
) (*raftpb.DeleteResponse, error) {

	if !s.Node.IsLeader() {
		return &raftpb.DeleteResponse{
			Success:       false,
			Message:       "This node is not the leader",
			LeaderAddress: s.getLeaderAddress(),
		}, nil
	}

	command := Command{
		Type: CommandDelete,
		Key:  req.Key,
	}

	success := s.Node.ReplicateCommand(command)

	if !success {
		return &raftpb.DeleteResponse{
			Success: false,
			Message: "Failed to reach Raft majority",
		}, nil
	}

	return &raftpb.DeleteResponse{
		Success: true,
		Message: "DELETE committed successfully",
	}, nil
}
func (s *GRPCServer) InstallSnapshot(
	ctx context.Context,
	req *raftpb.InstallSnapshotRequest,
) (*raftpb.InstallSnapshotResponse, error) {

	store := make(
		map[string]string,
		len(req.Store),
	)

	for key, value := range req.Store {
		store[key] = value
	}

	args := InstallSnapshotArgs{
		Term:              int(req.Term),
		LeaderID:          int(req.LeaderId),
		LastIncludedIndex: int(req.LastIncludedIndex),
		LastIncludedTerm:  int(req.LastIncludedTerm),
		Store:             store,
	}

	reply := s.Node.InstallSnapshot(args)

	return &raftpb.InstallSnapshotResponse{
		Term:    int32(reply.Term),
		Success: reply.Success,
	}, nil
}
