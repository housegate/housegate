// Package checkpoint persists the publisher's "where I left off" state
// across restarts. Atomic write via tmp+rename so a crash mid-write
// cannot corrupt the file.
package checkpoint

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"time"
)

type Checkpoint struct {
	LastModificationTime time.Time `json:"last_modification_time"`
}

// Load returns the checkpoint stored at path, or a zero-value
// Checkpoint if the file does not yet exist.
func Load(path string) (Checkpoint, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return Checkpoint{}, nil
	}
	if err != nil {
		return Checkpoint{}, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return Checkpoint{}, err
	}
	return cp, nil
}

// Save writes the checkpoint atomically: write tmp, fsync, rename.
func Save(path string, cp Checkpoint) error {
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
