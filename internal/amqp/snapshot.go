package amqp

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"erionn-mq/internal/amqpcore"
)

// Snapshot is a shared read model for management endpoints and metrics.
type Snapshot struct {
	GeneratedAt time.Time               `json:"generated_at"`
	Broker      amqpcore.BrokerSnapshot `json:"broker"`
	Connections []ConnectionView        `json:"connections"`
	Channels    []ChannelView           `json:"channels"`
}

type ConnectionView struct {
	Name     string `json:"name"`
	VHost    string `json:"vhost"`
	User     string `json:"user"`
	Protocol string `json:"protocol"`
	PeerHost string `json:"peer_host"`
	PeerPort int    `json:"peer_port"`
	State    string `json:"state"`
	Channels int    `json:"channels"`
}

type ChannelView struct {
	Name             string `json:"name"`
	Number           uint16 `json:"number"`
	ConnectionName   string `json:"connection_name"`
	PrefetchCount    int    `json:"prefetch_count"`
	ConsumerPrefetch int    `json:"consumer_prefetch_count"`
	Consumers        int    `json:"consumers"`
	Unacked          int    `json:"unacked"`
}

// Snapshot returns a consistent snapshot of broker + connection state.
func (s *Server) Snapshot() Snapshot {
	s.mu.Lock()
	conns := make([]*serverConn, 0, len(s.connections))
	for _, conn := range s.connections {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	brokerSnap := s.broker.Snapshot()
	queueConsumers := make(map[string]int)
	connections := make([]ConnectionView, 0, len(conns))
	channels := make([]ChannelView, 0)

	for _, conn := range conns {
		connName := formatConnName(conn.netConn)
		chans := conn.snapshotChannels(connName, queueConsumers)
		connections = append(connections, ConnectionView{
			Name:     connName,
			VHost:    normalizeVHost(conn.amqpConn.VHost),
			User:     "guest",
			Protocol: "AMQP 0-9-1",
			PeerHost: conn.peerHost(),
			PeerPort: conn.peerPort(),
			State:    "running",
			Channels: len(chans),
		})
		channels = append(channels, chans...)
	}

	for i := range brokerSnap.Queues {
		q := &brokerSnap.Queues[i]
		q.Consumers = queueConsumers[q.Name]
	}

	return Snapshot{
		GeneratedAt: time.Now().UTC(),
		Broker:      brokerSnap,
		Connections: connections,
		Channels:    channels,
	}
}

func (c *serverConn) snapshotChannels(connName string, queueConsumers map[string]int) []ChannelView {
	c.mu.Lock()
	channels := make([]*channelState, 0, len(c.channels))
	for _, ch := range c.channels {
		channels = append(channels, ch)
	}
	c.mu.Unlock()

	views := make([]ChannelView, 0, len(channels))
	for _, ch := range channels {
		view, consumers := ch.snapshot(connName)
		views = append(views, view)
		for _, queueName := range consumers {
			queueConsumers[queueName]++
		}
	}
	return views
}

func (ch *channelState) snapshot(connName string) (ChannelView, []string) {
	ch.mu.Lock()
	consumers := make([]string, 0, len(ch.consumers))
	for _, consumer := range ch.consumers {
		consumers = append(consumers, consumer.queueName)
	}
	view := ChannelView{
		Name:             fmt.Sprintf("%s (%d)", connName, ch.channel.ID),
		Number:           ch.channel.ID,
		ConnectionName:   connName,
		PrefetchCount:    ch.channel.PrefetchCount,
		ConsumerPrefetch: ch.channel.ConsumerPrefetchCount,
		Consumers:        len(ch.consumers),
		Unacked:          len(ch.inFlight),
	}
	ch.mu.Unlock()
	return view, consumers
}

func formatConnName(conn net.Conn) string {
	if conn == nil {
		return "unknown"
	}
	remote := conn.RemoteAddr().String()
	local := conn.LocalAddr().String()
	return fmt.Sprintf("%s -> %s", remote, local)
}

func (c *serverConn) peerHost() string {
	if c.netConn == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(c.netConn.RemoteAddr().String())
	if err != nil {
		return c.netConn.RemoteAddr().String()
	}
	return host
}

func (c *serverConn) peerPort() int {
	if c.netConn == nil {
		return 0
	}
	_, port, err := net.SplitHostPort(c.netConn.RemoteAddr().String())
	if err != nil {
		return 0
	}
	value, err := strconv.Atoi(port)
	if err != nil {
		return 0
	}
	return value
}

func normalizeVHost(vhost string) string {
	if vhost == "" {
		return "/"
	}
	if strings.HasPrefix(vhost, "/") {
		return vhost
	}
	return "/" + vhost
}
