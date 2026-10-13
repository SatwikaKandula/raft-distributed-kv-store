package raft

import (
	"context"
	"fmt"

	raftpb "raft-kv-store/proto"
)

type AdminGRPCServer struct {
	raftpb.UnimplementedAdminServiceServer

	Node *Node
}

func (s *AdminGRPCServer) BlockPeer(
	ctx context.Context,
	req *raftpb.BlockPeerRequest,
) (*raftpb.BlockPeerResponse, error) {

	peerID := int(req.PeerId)

	s.Node.BlockPeer(peerID)

	return &raftpb.BlockPeerResponse{
		Success: true,
		Message: fmt.Sprintf(
			"Node %d blocked Node %d",
			s.Node.ID,
			peerID,
		),
	}, nil
}

func (s *AdminGRPCServer) UnblockPeer(
	ctx context.Context,
	req *raftpb.UnblockPeerRequest,
) (*raftpb.UnblockPeerResponse, error) {

	peerID := int(req.PeerId)

	s.Node.UnblockPeer(peerID)

	return &raftpb.UnblockPeerResponse{
		Success: true,
		Message: fmt.Sprintf(
			"Node %d unblocked Node %d",
			s.Node.ID,
			peerID,
		),
	}, nil
}
