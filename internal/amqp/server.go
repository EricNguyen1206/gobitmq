package amqp

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"erionn-mq/internal/core"
	"erionn-mq/internal/store"
)

var errConnectionClosed = errors.New("amqp: connection closed")

type Server struct {
	Addr   string
	broker *core.Broker

	nextConnID     atomic.Uint64
	nextQueueID    atomic.Uint64
	nextConsumerID atomic.Uint64

	mu          sync.Mutex
	connections map[uint64]*serverConn
}

func NewServer(addr string, broker *core.Broker) *Server {
	if addr == "" {
		addr = DefaultAddr
	}
	if broker == nil {
		broker = core.NewBroker(func() store.MessageStore {
			return store.NewMemoryMessageStore()
		})
	}
	return &Server{
		Addr:        addr,
		broker:      broker,
		connections: make(map[uint64]*serverConn),
	}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

func (s *Server) Serve(ln net.Listener) error {
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(netConn net.Conn) {
	conn := s.newConn(netConn)
	defer conn.close()

	if err := conn.serve(); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, errConnectionClosed) {
		_ = conn.netConn.Close()
	}
}

func (s *Server) newConn(netConn net.Conn) *serverConn {
	id := s.nextConnID.Add(1)
	conn := &serverConn{
		server:  s,
		broker:  s.broker,
		netConn: netConn,
		amqpConn: &core.Connection{
			ID:   id,
			Conn: netConn,
		},
		channels: make(map[uint16]*channelState),
		frameMax: defaultFrameMax,
		done:     make(chan struct{}),
	}

	s.mu.Lock()
	s.connections[id] = conn
	s.mu.Unlock()

	return conn
}

func (s *Server) consumerCount(queueName string) uint32 {
	s.mu.Lock()
	conns := make([]*serverConn, 0, len(s.connections))
	for _, conn := range s.connections {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	var count uint32
	for _, conn := range conns {
		conn.mu.Lock()
		channels := make([]*channelState, 0, len(conn.channels))
		for _, ch := range conn.channels {
			channels = append(channels, ch)
		}
		conn.mu.Unlock()

		for _, ch := range channels {
			count += ch.consumerCount(queueName)
		}
	}
	return count
}
