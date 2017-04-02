package transport

import (
	"fmt"
	"io"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	paxospb "github.com/nvanbenschoten/paxos/paxos/paxospb"
	transpb "github.com/nvanbenschoten/paxos/transport/transportpb"
)

// UpdateRequest represents a request to perform a client update. It includes
// a channel to return the globally ordered result on.
type UpdateRequest struct {
	Update  *paxospb.ClientUpdate
	ReturnC chan<- paxospb.GloballyOrderedUpdate
}

// PaxosServer handles internal and external RPC messages for a Paxos node.
type PaxosServer struct {
	msgC chan *paxospb.Message
	upC  chan UpdateRequest

	lis        net.Listener
	grpcServer *grpc.Server
}

// NewPaxosServer creates a new PaxosServer.
func NewPaxosServer(port int) (*PaxosServer, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	ps := &PaxosServer{
		msgC:       make(chan *paxospb.Message, 16),
		upC:        make(chan UpdateRequest, 16),
		lis:        lis,
		grpcServer: grpc.NewServer(),
	}
	transpb.RegisterPaxosTransportServer(ps.grpcServer, ps)
	transpb.RegisterClientServiceServer(ps.grpcServer, ps)
	return ps, nil
}

// DeliverMessage implements the PaxosTransportServer interface. It receives
// each message from the client stream and passes it to the server's message
// channel.
func (ps *PaxosServer) DeliverMessage(
	stream transpb.PaxosTransport_DeliverMessageServer,
) error {
	ctx := stream.Context()
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return stream.SendAndClose(&transpb.Empty{})
			}
			return err
		}
		select {
		case ps.msgC <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ClientUpdate implements the ClientServiceServer interface. It receives
// the ClientUpdate from the client and passes it as an UpdateRequest on
// the server's update channel. The method will block until the update is
// globally ordered.
func (ps *PaxosServer) ClientUpdate(
	ctx context.Context, clientUpdate *paxospb.ClientUpdate,
) (*paxospb.GloballyOrderedUpdate, error) {
	ret := make(chan paxospb.GloballyOrderedUpdate, 1)
	ps.upC <- UpdateRequest{
		Update:  clientUpdate,
		ReturnC: ret,
	}
	select {
	case gou := <-ret:
		return &gou, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Msgs returns the channel that all Paxos messages will be delivered from
// the server on.
func (ps *PaxosServer) Msgs() <-chan *paxospb.Message {
	return ps.msgC
}

// Updates returns the channel that all client updates will be delivered from
// the server on.
func (ps *PaxosServer) Updates() <-chan UpdateRequest {
	return ps.upC
}

// Serve begins serving on server, blocking until Stop is called or an error
// is observed.
func (ps *PaxosServer) Serve() error {
	return ps.grpcServer.Serve(ps.lis)
}

// Stop stops the PaxosServer.
func (ps *PaxosServer) Stop() {
	close(ps.msgC)
	ps.grpcServer.Stop()
}
