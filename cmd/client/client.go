package main

import (
	"context"
	"log"
	"math/rand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/pkg/errors"

	"github.com/nvanbenschoten/paxos/cmd/util"
	pb "github.com/nvanbenschoten/paxos/paxos/paxospb"
	"github.com/nvanbenschoten/paxos/transport"
)

type client struct {
	id        uint64
	seqNum    uint64
	serverSet map[*transport.ExternalClient]struct{}
}

func newClient(addrs []util.Addr) (*client, error) {
	serverSet := make(map[*transport.ExternalClient]struct{}, len(addrs))
	for _, addr := range addrs {
		addrStr := addr.AddrStr()
		c, err := transport.NewExternalClient(addrStr)
		if err != nil {
			log.Printf("could not connect to %s", addrStr)
		} else {
			serverSet[c] = struct{}{}
		}
	}
	if len(serverSet) == 0 {
		return nil, errors.Errorf("no servers available")
	}

	return &client{
		id:        rand.Uint64(),
		serverSet: serverSet,
	}, nil
}

func (c *client) sendUpdate(ctx context.Context, update []byte) (*pb.GloballyOrderedUpdate, error) {
	up := &pb.ClientUpdate{
		ClientId:  c.id,
		Timestamp: c.nextSeqNumber(),
		Update:    update,
	}
	s := c.randomServer()
	gou, err := s.ClientUpdate(ctx, up)
	if err != nil {
		if grpc.Code(err) == codes.Unavailable {
			// If the node is down, remove it from the server set.
			log.Printf("detected node unavailable")
			delete(c.serverSet, s)
			s.Close()

			// Make sure we still have at least one server available.
			if len(c.serverSet) == 0 {
				log.Fatal("no servers available")
			}
		}
		return nil, err
	}
	return gou, nil
}

func (c *client) nextSeqNumber() uint64 {
	c.seqNum++
	return c.seqNum
}

func (c *client) randomServer() *transport.ExternalClient {
	i := rand.Intn(len(c.serverSet))
	for c := range c.serverSet {
		if i == 0 {
			return c
		}
		i--
	}
	panic("unreachable")
}
