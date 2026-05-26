// Package chexport runs `clickhouse-client --format Parquet` as a
// subprocess and returns its stdout. Kept dead-simple on purpose:
// shelling out beats wiring an HTTP client + Parquet writer for an
// MVP measurement rig.
package chexport

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

// ExportPart returns the Parquet-encoded bytes of all rows in a single
// part of (database, table).
func ExportPart(ctx context.Context, cfg Config, database, table, partName string) ([]byte, error) {
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE _part = '%s' FORMAT Parquet", database, table, partName)
	return run(ctx, cfg, query, nil)
}

// QueryShowCreate returns the SHOW CREATE TABLE result as a single string.
func QueryShowCreate(ctx context.Context, cfg Config, database, table string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", database, table)
	out, err := run(ctx, cfg, query, nil)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// QueryNewParts returns part names with modification_time > sinceUnix.
func QueryNewParts(ctx context.Context, cfg Config, database, table string, sinceUnix int64) ([]Part, error) {
	query := fmt.Sprintf(
		"SELECT name, toUnixTimestamp(modification_time) FROM system.parts "+
			"WHERE database = '%s' AND table = '%s' AND active = 1 AND modification_time > toDateTime(%d) "+
			"ORDER BY modification_time, name "+
			"FORMAT TabSeparated",
		database, table, sinceUnix)
	out, err := run(ctx, cfg, query, nil)
	if err != nil {
		return nil, err
	}
	var parts []Part
	for _, line := range bytes.Split(bytes.TrimSpace(out), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		fields := bytes.SplitN(line, []byte("\t"), 2)
		if len(fields) != 2 {
			continue
		}
		ts, err := strconv.ParseInt(string(fields[1]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse modification_time: %w", err)
		}
		parts = append(parts, Part{Name: string(fields[0]), ModificationUnix: ts})
	}
	return parts, nil
}

type Part struct {
	Name             string
	ModificationUnix int64
}

// RunQuery is an exported wrapper around run() for callers that need
// arbitrary single-statement output (used by pkg/verify).
func RunQuery(ctx context.Context, cfg Config, query string) ([]byte, error) {
	return run(ctx, cfg, query, nil)
}

func run(ctx context.Context, cfg Config, query string, stdin []byte) ([]byte, error) {
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
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("clickhouse-client: %w: %s", err, stderr.String())
	}
	return stdout.Bytes(), nil
}
