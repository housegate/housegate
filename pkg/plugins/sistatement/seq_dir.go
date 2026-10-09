package sistatement

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// mkdirAllDurable creates dir and its missing ancestors with perm and fsyncs
// every new entry into its parent, so a crash cannot lose a directory a
// durable seq file was already renamed into (spec 2026-10-09 §6.4).
func mkdirAllDurable(dir string, perm fs.FileMode, openDir func(string) (seqDir, error)) error {
	dir = filepath.Clean(dir)
	var missing []string
	for p := dir; ; {
		info, err := os.Stat(p)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("sistatement: state dir %s is not a directory", p)
			}
			break
		}
		if errors.Is(err, fs.ErrPermission) {
			return fmt.Errorf("sistatement: cannot access state dir %s; check ownership and search permissions on parent directories for the agent user: %w", p, err)
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("sistatement: stat state dir %s: %w", p, err)
		}
		missing = append(missing, p)
		parent := filepath.Dir(p)
		if parent == p {
			break
		}
		p = parent
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], perm); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("sistatement: create state dir %s: %w", missing[i], err)
		}
		if err := syncDir(openDir, filepath.Dir(missing[i])); err != nil {
			return err
		}
	}
	return nil
}

func syncDir(openDir func(string) (seqDir, error), path string) error {
	dir, err := openDir(path)
	if err != nil {
		return fmt.Errorf("sistatement: open state dir %s for fsync: %w", path, err)
	}
	var errs []error
	if err := dir.Sync(); err != nil {
		errs = append(errs, fmt.Errorf("fsync state dir %s: %w", path, err))
	}
	if err := dir.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close state dir %s: %w", path, err))
	}
	return errors.Join(errs...)
}

// writeDurable replaces path with data: temp file, fsync, rename, directory
// fsync — the discipline the seq file has always used.
func writeDurable(openDir func(string) (seqDir, error), path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("sistatement: open %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("sistatement: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sistatement: fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("sistatement: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("sistatement: rename %s: %w", tmp, err)
	}
	return syncDir(openDir, filepath.Dir(path))
}
