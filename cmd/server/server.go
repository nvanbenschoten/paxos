package main

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/nvanbenschoten/paxos/paxos"
	pb "github.com/nvanbenschoten/paxos/paxos/paxospb"
	"github.com/nvanbenschoten/paxos/transport"
)

const (
	// tickInterval is the interval at which the Paxos state machine "ticks".
	tickInterval = 10 * time.Millisecond
)

type server struct {
	id             uint64
	node           paxos.Node
	logger         paxos.Logger
	ticker         *time.Ticker
	server         *transport.PaxosServer
	clients        map[uint64]*transport.PaxosClient
	unavailClients map[uint64]struct{}
	pendingUpdates map[clientUpdateKey]chan<- pb.GloballyOrderedUpdate
}

type clientUpdateKey struct {
	clientID  uint64
	timestamp uint64
}

func newServer(ph parsedHostfile) (*server, error) {
	// Create a new PaxosServer to listen on.
	ps, err := transport.NewPaxosServer(ph.myPort)
	if err != nil {
		return nil, err
	}

	// Create PaxosClients for each other host in the network.
	clients := make(map[uint64]*transport.PaxosClient, len(ph.peerAddrs))
	for _, addr := range ph.peerAddrs {
		pc, err := transport.NewPaxosClient(addr.AddrStr())
		if err != nil {
			return nil, err
		}
		clients[uint64(addr.Idx)] = pc
	}

	config := ph.toPaxosConfig()
	return &server{
		id:             config.ID,
		node:           paxos.StartNode(config),
		logger:         config.Logger,
		ticker:         time.NewTicker(tickInterval),
		server:         ps,
		clients:        clients,
		unavailClients: make(map[uint64]struct{}, len(ph.peerAddrs)),
		pendingUpdates: make(map[clientUpdateKey]chan<- pb.GloballyOrderedUpdate),
	}, nil
}

func (s *server) Stop() {
	s.ticker.Stop()
	for _, c := range s.clients {
		c.Close()
	}
	s.server.Stop()
	s.node.Stop()
}

func (s *server) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			select {
			case <-s.ticker.C:
				s.node.Tick()
			case m := <-s.server.Msgs():
				s.node.Step(ctx, *m)
			case upReq := <-s.server.Updates():
				s.registerClientUpdate(upReq)
				s.node.Propose(ctx, *upReq.Update)
			case rd := <-s.node.Ready():
				// saveToStorage(rd.State, rd.Entries, rd.Snapshot)

				if err := s.sendAll(ctx, rd.Messages); err != nil {
					s.logger.Warning(err)
				}

				s.handleOrderedUpdates(rd.OrderedUpdates)

				// n.Advance()
			case <-ctx.Done():
				return
			}
		}
	}()
	defer s.Stop()
	return s.server.Serve()
}

func (s *server) registerClientUpdate(upReq transport.UpdateRequest) {
	up := upReq.Update
	key := clientUpdateKey{clientID: up.ClientId, timestamp: up.Timestamp}
	s.pendingUpdates[key] = upReq.ReturnC
}

func (s *server) handleOrderedUpdates(ordered []pb.GloballyOrderedUpdate) {
	for _, gou := range ordered {
		up := gou.Update

		// This is where we would perform some deterministic update to the
		// server's state machine.
		s.logger.Infof("Applying update %q with seq %d", string(up.Update), gou.Seq)

		// If this was our server's client, we also need to inform the client
		// that their update has been ordered.
		if up.NodeId == s.id {
			key := clientUpdateKey{clientID: up.ClientId, timestamp: up.Timestamp}
			ret, ok := s.pendingUpdates[key]
			if !ok {
				s.logger.Panicf("ordered update for unknown client: %v", gou)
			}
			delete(s.pendingUpdates, key)
			ret <- gou
			close(ret)
		}
	}
}

func (s *server) sendAll(ctx context.Context, msgs []pb.Message) error {
	outboxes := make(map[uint64][]pb.Message)
	for _, m := range msgs {
		outboxes[m.To] = append(outboxes[m.To], m)
	}
	for to, toMsgs := range outboxes {
		if _, unavail := s.unavailClients[to]; unavail {
			continue
		}
		if err := s.sendAllTo(ctx, toMsgs, to); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) sendAllTo(ctx context.Context, msgs []pb.Message, to uint64) (err error) {
	c, ok := s.clients[to]
	if !ok {
		return errors.Errorf("message found with unknown destination: %v", to)
	}
	defer func() {
		if grpc.Code(err) == codes.Unavailable {
			// If the node is down, record that it's unavailable so that we
			// dont continue to sent to it.
			s.logger.Warningf("detected node %d unavailable", to)
			s.unavailClients[to] = struct{}{}
			c.Close()
		}
	}()
	stream, err := c.DeliverMessage(ctx)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if m.To != to {
			panic("unexpected destination")
		}
		if err := stream.Send(&m); err != nil {
			return err
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}
