//go:build !(linux || darwin)

package sistatement

func lockFile(path string) (func() error, error) { return lockUnsupported(path) }
