// Package chimport pipes a payload into clickhouse-client for INSERT.
package chimport

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
)

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
}

// Insert runs `clickhouse-client --query "INSERT INTO db.t FORMAT <fmt>"`
// with payload streamed on stdin. Use format "Parquet" for the rebuilder
// path and "TabSeparated" / "Values" for the loadgen path.
func Insert(ctx context.Context, cfg Config, database, table, format string, payload []byte) error {
	query := fmt.Sprintf("INSERT INTO `%s`.`%s` FORMAT %s", database, table, format)
	args := []string{
		"--host", cfg.Host,
		"--port", strconv.Itoa(cfg.Port),
		"--query", query,
	}
	if cfg.User != "" {
		args = append(args, "--user", cfg.User)
	}
	if cfg.Password != "" {
		args = append(args, "--password", cfg.Password)
	}
	cmd := exec.CommandContext(ctx, "clickhouse-client", args...)
	cmd.Stdin = bytes.NewReader(payload)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("clickhouse-client INSERT: %w: %s", err, stderr.String())
	}
	return nil
}

// (No DDL helper here — the MVP runs DDL via shell `clickhouse-client < setup.sql`.)
