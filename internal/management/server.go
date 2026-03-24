package management

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"

	"erionn-mq/internal/amqp"
	"erionn-mq/internal/amqpcore"
)

const DefaultAddr = ":15672"

type Role string

const (
	RoleAdmin      Role = "admin"
	RoleMonitoring Role = "monitoring"
	RoleManagement Role = "management"
)

type User struct {
	Username string
	Password string
	Role     Role
}

type Server struct {
	Addr   string
	Broker *amqpcore.Broker
	AMQP   *amqp.Server
	users  map[string]User
}

func NewServer(addr string, broker *amqpcore.Broker, amqpServer *amqp.Server) *Server {
	if addr == "" {
		addr = DefaultAddr
	}
	if broker == nil {
		broker = amqpcore.NewBroker(nil)
	}
	return &Server{
		Addr:   addr,
		Broker: broker,
		AMQP:   amqpServer,
		users: map[string]User{
			"guest": {Username: "guest", Password: "guest", Role: RoleAdmin},
		},
	}
}

func (s *Server) ListenAndServe() error {
	return http.ListenAndServe(s.Addr, s.routes())
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/overview", s.withAuth(RoleMonitoring, s.handleOverview))
	mux.HandleFunc("/api/connections", s.withAuth(RoleMonitoring, s.handleConnections))
	mux.HandleFunc("/api/channels", s.withAuth(RoleMonitoring, s.handleChannels))
	mux.HandleFunc("/api/exchanges", s.withAuth(RoleMonitoring, s.handleExchanges))
	mux.HandleFunc("/api/queues", s.withAuth(RoleMonitoring, s.handleQueues))
	mux.HandleFunc("/api/bindings", s.withAuth(RoleMonitoring, s.handleBindings))
	mux.HandleFunc("/api/exchanges/", s.withAuth(RoleManagement, s.handleExchangeDeclare))
	mux.HandleFunc("/api/queues/", s.withAuth(RoleManagement, s.handleQueueMutations))
	return mux
}

func (s *Server) withAuth(minRole Role, handler func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !isLocalhost(r.RemoteAddr) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		user, err := s.authenticate(r)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Basic realm="management"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if !roleAllows(user.Role, minRole, r.Method) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		handler(w, r)
	}
}

func (s *Server) authenticate(r *http.Request) (User, error) {
	username, password, ok := r.BasicAuth()
	if !ok {
		return User{}, errors.New("missing auth")
	}
	user, ok := s.users[username]
	if !ok || user.Password != password {
		return User{}, errors.New("invalid credentials")
	}
	return user, nil
}

func roleAllows(userRole Role, minRole Role, method string) bool {
	if userRole == RoleAdmin {
		return true
	}
	if method == http.MethodGet {
		if minRole == RoleManagement {
			return userRole == RoleManagement
		}
		return userRole == RoleMonitoring || userRole == RoleManagement
	}
	return userRole == RoleManagement
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snap := s.snapshot()
	resp := overviewResponse{
		ManagementVersion: "0.1.0",
		Node:              "erionn@localhost",
		ObjectTotals: objectTotals{
			Connections: len(snap.Connections),
			Channels:    len(snap.Channels),
			Exchanges:   len(snap.Broker.Exchanges),
			Queues:      len(snap.Broker.Queues),
			Bindings:    len(snap.Broker.Bindings),
		},
		MessageStats: snap.Broker.MessageStats,
	}
	writeJSON(w, resp)
}

func (s *Server) handleConnections(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snap := s.snapshot()
	writeJSON(w, snap.Connections)
}

func (s *Server) handleChannels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snap := s.snapshot()
	writeJSON(w, snap.Channels)
}

func (s *Server) handleExchanges(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snap := s.snapshot()
	writeJSON(w, snap.Broker.Exchanges)
}

func (s *Server) handleQueues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snap := s.snapshot()
	writeJSON(w, snap.Broker.Queues)
}

func (s *Server) handleBindings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snap := s.snapshot()
	writeJSON(w, snap.Broker.Bindings)
}

func (s *Server) handleExchangeDeclare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	vhost, name, ok := parseEntityPath(r.URL.Path, "/api/exchanges/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	if vhost != "/" {
		http.Error(w, "vhost not supported", http.StatusNotFound)
		return
	}

	var req exchangeDeclareRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
	}
	if req.Type == "" {
		req.Type = "direct"
	}
	kind, err := parseExchangeType(req.Type)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := s.Broker.DeclareExchange(name, kind, req.Durable, req.AutoDelete, req.Internal); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleQueueMutations(w http.ResponseWriter, r *http.Request) {
	vhost, name, ok := parseEntityPath(r.URL.Path, "/api/queues/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	if vhost != "/" {
		http.Error(w, "vhost not supported", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPut:
		var req queueDeclareRequest
		if r.ContentLength > 0 {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "invalid json", http.StatusBadRequest)
				return
			}
		}
		if _, err := s.Broker.DeclareQueue(name, req.Durable, req.Exclusive, req.AutoDelete, req.Arguments); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		if err := s.Broker.DeleteQueue(name); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

type overviewResponse struct {
	ManagementVersion string                `json:"management_version"`
	Node              string                `json:"node"`
	ObjectTotals      objectTotals          `json:"object_totals"`
	MessageStats      amqpcore.MessageStats `json:"message_stats"`
}

type objectTotals struct {
	Connections int `json:"connections"`
	Channels    int `json:"channels"`
	Exchanges   int `json:"exchanges"`
	Queues      int `json:"queues"`
	Bindings    int `json:"bindings"`
}

type exchangeDeclareRequest struct {
	Type       string         `json:"type"`
	Durable    bool           `json:"durable"`
	AutoDelete bool           `json:"auto_delete"`
	Internal   bool           `json:"internal"`
	Arguments  map[string]any `json:"arguments"`
}

type queueDeclareRequest struct {
	Durable    bool           `json:"durable"`
	AutoDelete bool           `json:"auto_delete"`
	Exclusive  bool           `json:"exclusive"`
	Arguments  map[string]any `json:"arguments"`
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(value)
}

func parseEntityPath(rawPath, prefix string) (string, string, bool) {
	if !strings.HasPrefix(rawPath, prefix) {
		return "", "", false
	}
	trimmed := strings.TrimPrefix(rawPath, prefix)
	clean := path.Clean("/" + trimmed)
	parts := strings.Split(strings.TrimPrefix(clean, "/"), "/")
	if len(parts) < 2 {
		return "", "", false
	}
	vhost, err := url.PathUnescape(parts[0])
	if err != nil {
		return "", "", false
	}
	name, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", "", false
	}
	if vhost == "" {
		vhost = "/"
	}
	return vhost, name, true
}

func isLocalhost(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return host == "localhost"
	}
	return ip.IsLoopback()
}

func parseExchangeType(value string) (amqpcore.ExchangeType, error) {
	switch amqpcore.ExchangeType(value) {
	case amqpcore.ExchangeDirect, amqpcore.ExchangeFanout, amqpcore.ExchangeTopic:
		return amqpcore.ExchangeType(value), nil
	case amqpcore.ExchangeHeaders:
		return "", errors.New("exchange type headers is not supported")
	default:
		return "", errors.New("unsupported exchange type")
	}
}

func (s *Server) snapshot() amqp.Snapshot {
	if s.AMQP != nil {
		return s.AMQP.Snapshot()
	}
	return amqp.Snapshot{Broker: s.Broker.Snapshot()}
}
