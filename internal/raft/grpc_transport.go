package raft

import (
	"fmt"
	"net"

	raftpb "raft-kv-store/proto"

	"google.golang.org/grpc"
)

func (n *Node) StartGRPCServer(address string) error {

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	//"""net.Listen()	= opens the door
	//	grpc.NewServer() = creates the person/system that will handle people coming through that door"""

	//When someone calls over the network send that request to GRPCServer.
	//For the RaftService defined in the proto file, use this GRPCServer object to handle its functions.
	/////////gRPC is the the OBJECT of GRPCServer
	// Register GRPCServer as the handler for the RaftService.
	// This tells gRPC to send incoming RequestVote and AppendEntries RPCs
	// to GRPCServer.RequestVote() and GRPCServer.AppendEntries().
	// Raft internal RPCs.
	raftpb.RegisterRaftServiceServer(grpcServer, &GRPCServer{Node: n})

	// Client PUT / GET / DELETE RPCs.
	raftpb.RegisterKVServiceServer(grpcServer, &KVGRPCServer{Node: n})
	raftpb.RegisterAdminServiceServer(grpcServer, &AdminGRPCServer{Node: n})
	fmt.Printf(
		"Node %d gRPC server listening on %s\n",
		n.ID,
		address,
	)

	return grpcServer.Serve(listener)
}
