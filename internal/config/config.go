package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"erionn-mq/internal/amqp"
	"erionn-mq/internal/management"
)

const (
	envAMQPAddr        = "ERIONN_AMQP_ADDR"
	envManagementAddr  = "ERIONN_MGMT_ADDR"
	envDataDir         = "ERIONN_DATA_DIR"
	envManagementUsers = "ERIONN_MGMT_USERS"
	envManagementAllow = "ERIONN_MGMT_ALLOW_REMOTE"
)

type Config struct {
	AMQPAddr              string
	ManagementAddr        string
	DataDir               string
	ManagementAllowRemote bool
	ManagementUsers       []management.User
}

func Load() (Config, error) {
	cfg := Config{
		AMQPAddr:        envOr(envAMQPAddr, amqp.DefaultAddr),
		ManagementAddr:  envOr(envManagementAddr, management.DefaultAddr),
		DataDir:         envOr(envDataDir, filepath.Join("data", "broker")),
		ManagementUsers: management.DefaultUsers(),
	}

	if value := strings.TrimSpace(os.Getenv(envManagementAllow)); value != "" {
		allowed, err := parseBool(value)
		if err != nil {
			return cfg, err
		}
		cfg.ManagementAllowRemote = allowed
	}

	if value := strings.TrimSpace(os.Getenv(envManagementUsers)); value != "" {
		users, err := parseUsers(value)
		if err != nil {
			return cfg, err
		}
		cfg.ManagementUsers = users
	}

	return cfg, nil
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func parseUsers(raw string) ([]management.User, error) {
	chunks := strings.Split(raw, ",")
	users := make([]management.User, 0, len(chunks))
	for _, chunk := range chunks {
		entry := strings.TrimSpace(chunk)
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, ":")
		if len(parts) < 2 || len(parts) > 3 {
			return nil, fmt.Errorf("config: invalid user entry %q", entry)
		}
		username := strings.TrimSpace(parts[0])
		password := strings.TrimSpace(parts[1])
		if username == "" || password == "" {
			return nil, fmt.Errorf("config: invalid user entry %q", entry)
		}
		role := management.RoleAdmin
		if len(parts) == 3 {
			var err error
			role, err = parseRole(parts[2])
			if err != nil {
				return nil, err
			}
		}
		users = append(users, management.User{Username: username, Password: password, Role: role})
	}
	if len(users) == 0 {
		return nil, fmt.Errorf("config: no valid users found")
	}
	return users, nil
}

func parseRole(value string) (management.Role, error) {
	role := strings.ToLower(strings.TrimSpace(value))
	if role == "" {
		return management.RoleAdmin, nil
	}
	switch management.Role(role) {
	case management.RoleAdmin, management.RoleMonitoring, management.RoleManagement:
		return management.Role(role), nil
	default:
		return "", fmt.Errorf("config: unsupported management role %q", value)
	}
}

func parseBool(value string) (bool, error) {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	switch trimmed {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("config: invalid boolean value %q", value)
	}
}
