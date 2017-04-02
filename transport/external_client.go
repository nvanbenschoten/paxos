package transport

import (
	"google.golang.org/grpc"

	transpb "github.com/nvanbenschoten/paxos/transport/transportpb"
)

// ExternalClient is a client stub implementing the ClientServiceClient
// interface.
type ExternalClient struct {
	transpb.ClientServiceClient
	*grpc.ClientConn
}

// NewExternalClient creates a new PaxosClient.
func NewExternalClient(addr string) (*ExternalClient, error) {
	conn, err := grpc.Dial(addr, clientOpts...)
	if err != nil {
		return nil, err
	}
	client := transpb.NewClientServiceClient(conn)
	return &ExternalClient{client, conn}, nil
}
