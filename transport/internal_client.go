package transport

import (
	"time"

	"google.golang.org/grpc"

	transpb "github.com/nvanbenschoten/paxos/transport/transportpb"
)

var clientOpts = []grpc.DialOption{
	grpc.WithInsecure(),
	grpc.WithBlock(),
	grpc.WithTimeout(3 * time.Second),
}

// PaxosClient is a client stub implementing the PaxosTransportClient
// interface.
type PaxosClient struct {
	transpb.PaxosTransportClient
	*grpc.ClientConn
}

// NewPaxosClient creates a new PaxosClient.
func NewPaxosClient(addr string) (*PaxosClient, error) {
	conn, err := grpc.Dial(addr, clientOpts...)
	if err != nil {
		return nil, err
	}
	client := transpb.NewPaxosTransportClient(conn)
	return &PaxosClient{client, conn}, nil
}
