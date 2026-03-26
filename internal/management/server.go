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
	"erionn-mq/internal/core"
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
	addr   string
	broker *core.Broker
	amqp   *amqp.Server
	users  map[string]User

	AllowRemote bool
}

type Config struct {
	Addr        string
	Users       []User
	AllowRemote bool
}

func NewServerWithConfig(cfg Config, broker *core.Broker, amqpServer *amqp.Server) *Server {
	addr := cfg.Addr
	if addr == "" {
		addr = DefaultAddr
	}
	if broker == nil {
		broker = core.NewBroker(nil)
	}
	users := cfg.Users
	if len(users) == 0 {
		users = DefaultUsers()
	}
	return &Server{
		addr:        addr,
		broker:      broker,
		amqp:        amqpServer,
		users:       mapUsers(users),
		AllowRemote: cfg.AllowRemote,
	}
}

func (s *Server) ListenAndServe() error {
	return http.ListenAndServe(s.addr, s.routes())
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/overview", s.handleRead(RoleMonitoring, func(snap amqp.Snapshot) any {
		return newOverviewResponse(snap)
	}))
	mux.HandleFunc("/api/connections", s.handleRead(RoleMonitoring, func(snap amqp.Snapshot) any {
		return snap.Connections
	}))
	mux.HandleFunc("/api/channels", s.handleRead(RoleMonitoring, func(snap amqp.Snapshot) any {
		return snap.Channels
	}))
	mux.HandleFunc("/api/exchanges", s.handleRead(RoleMonitoring, func(snap amqp.Snapshot) any {
		return snap.Broker.Exchanges
	}))
	mux.HandleFunc("/api/queues", s.handleRead(RoleMonitoring, func(snap amqp.Snapshot) any {
		return snap.Broker.Queues
	}))
	mux.HandleFunc("/api/bindings", s.handleRead(RoleMonitoring, func(snap amqp.Snapshot) any {
		return snap.Broker.Bindings
	}))
	mux.HandleFunc("/api/exchanges/", s.withAuth(RoleManagement, s.handleExchangeDeclare))
	mux.HandleFunc("/api/queues/", s.withAuth(RoleManagement, s.handleQueueMutations))
	return mux
}

func (s *Server) handleRead(minRole Role, view func(amqp.Snapshot) any) http.HandlerFunc {
	return s.withAuth(minRole, func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		writeJSON(w, view(s.snapshot()))
	})
}

func (s *Server) withAuth(minRole Role, handler func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.AllowRemote && !isLocalhost(r.RemoteAddr) {
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

func (s *Server) handleExchangeDeclare(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPut) {
		return
	}
	name, ok := entityName(w, r, "/api/exchanges/")
	if !ok {
		return
	}
	var req exchangeDeclareRequest
	if err := decodeOptionalJSON(r, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.Type == "" {
		req.Type = "direct"
	}
	kind, err := core.ParseExchangeType(req.Type)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := s.broker.DeclareExchange(name, kind, req.Durable, req.AutoDelete, req.Internal); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleQueueMutations(w http.ResponseWriter, r *http.Request) {
	name, ok := entityName(w, r, "/api/queues/")
	if !ok {
		return
	}
	switch r.Method {
	case http.MethodPut:
		var req queueDeclareRequest
		if err := decodeOptionalJSON(r, &req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if _, err := s.broker.DeclareQueue(name, req.Durable, req.Exclusive, req.AutoDelete, req.Arguments); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		if err := s.broker.DeleteQueue(name); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

type overviewResponse struct {
	ManagementVersion string            `json:"management_version"`
	Node              string            `json:"node"`
	ObjectTotals      objectTotals      `json:"object_totals"`
	MessageStats      core.MessageStats `json:"message_stats"`
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

func DefaultUsers() []User {
	return []User{{Username: "guest", Password: "guest", Role: RoleAdmin}}
}

func newOverviewResponse(snap amqp.Snapshot) overviewResponse {
	return overviewResponse{
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
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func decodeOptionalJSON(r *http.Request, dst any) error {
	if r.ContentLength <= 0 {
		return nil
	}
	return json.NewDecoder(r.Body).Decode(dst)
}

func entityName(w http.ResponseWriter, r *http.Request, prefix string) (string, bool) {
	vhost, name, ok := parseEntityPath(r.URL.Path, prefix)
	if !ok {
		http.NotFound(w, r)
		return "", false
	}
	if vhost != "/" {
		http.Error(w, "vhost not supported", http.StatusNotFound)
		return "", false
	}
	return name, true
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

func mapUsers(users []User) map[string]User {
	mapped := make(map[string]User, len(users))
	for _, user := range users {
		mapped[user.Username] = user
	}
	return mapped
}

func (s *Server) snapshot() amqp.Snapshot {
	if s.amqp != nil {
		return s.amqp.Snapshot()
	}
	return amqp.Snapshot{Broker: s.broker.Snapshot()}
}
