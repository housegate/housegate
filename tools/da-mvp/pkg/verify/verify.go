// Package verify runs post-rebuild correctness checks comparing target
// against source ClickHouse.
package verify

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/housegate/housegate/tools/da-mvp/pkg/chexport"
)

type Mode int

const (
	ModeCount Mode = iota
	ModeSample
	ModeFull
)

func ParseMode(s string) (Mode, error) {
	switch strings.ToLower(s) {
	case "count":
		return ModeCount, nil
	case "sample":
		return ModeSample, nil
	case "full":
		return ModeFull, nil
	default:
		return 0, fmt.Errorf("unknown verify mode %q", s)
	}
}

// Run executes mode on both source and target and returns a diff
// description (empty string = match).
func Run(ctx context.Context, mode Mode, src, dst chexport.Config, database, table string) (string, error) {
	switch mode {
	case ModeCount:
		return runCount(ctx, src, dst, database, table)
	case ModeSample:
		return runSample(ctx, src, dst, database, table)
	case ModeFull:
		return runFull(ctx, src, dst, database, table)
	}
	return "", fmt.Errorf("invalid mode")
}

func runCount(ctx context.Context, src, dst chexport.Config, db, tbl string) (string, error) {
	q := fmt.Sprintf("SELECT count() FROM `%s`.`%s` FORMAT TabSeparated", db, tbl)
	srcN, err := chexport.RunQuery(ctx, src, q)
	if err != nil {
		return "", err
	}
	dstN, err := chexport.RunQuery(ctx, dst, q)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(srcN, dstN) {
		return fmt.Sprintf("count mismatch: src=%s dst=%s", bytes.TrimSpace(srcN), bytes.TrimSpace(dstN)), nil
	}
	return "", nil
}

func runSample(ctx context.Context, src, dst chexport.Config, db, tbl string) (string, error) {
	q := fmt.Sprintf(
		"SELECT cityHash64(toString(tuple(*))) FROM (SELECT * FROM `%s`.`%s` ORDER BY tuple(*) LIMIT 1000) FORMAT TabSeparated",
		db, tbl)
	srcN, err := chexport.RunQuery(ctx, src, q)
	if err != nil {
		return "", err
	}
	dstN, err := chexport.RunQuery(ctx, dst, q)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(srcN, dstN) {
		return "sample hash mismatch", nil
	}
	return "", nil
}

func runFull(ctx context.Context, src, dst chexport.Config, db, tbl string) (string, error) {
	q := fmt.Sprintf(
		"SELECT sum(cityHash64(toString(tuple(*)))) FROM `%s`.`%s` FORMAT TabSeparated",
		db, tbl)
	srcN, err := chexport.RunQuery(ctx, src, q)
	if err != nil {
		return "", err
	}
	dstN, err := chexport.RunQuery(ctx, dst, q)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(srcN, dstN) {
		return fmt.Sprintf("full hash mismatch: src=%s dst=%s", bytes.TrimSpace(srcN), bytes.TrimSpace(dstN)), nil
	}
	return "", nil
}
