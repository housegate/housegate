//go:build linux || darwin

package sistatement

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestLanePoolHelperProcess is re-executed by TestAcquireFromTwoProcesses; it
// holds one lane until its stdin closes.
func TestLanePoolHelperProcess(t *testing.T) {
	dir := os.Getenv("SISTATEMENT_LANE_HELPER_DIR")
	if dir == "" {
		t.Skip("helper process only")
	}
	p, err := OpenLanePool(dir, LanePoolOptions{})
	if err != nil {
		fmt.Println("ERR", err)
		os.Exit(1)
	}
	s, _, err := p.Acquire()
	if err != nil {
		fmt.Println("ERR", err)
		os.Exit(1)
	}
	fmt.Println("LANE", s.Lane())
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

func TestAcquireFromTwoProcesses(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLanePoolHelperProcess$")
	cmd.Env = append(os.Environ(), "SISTATEMENT_LANE_HELPER_DIR="+dir)
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "LANE ") {
		t.Fatalf("helper: %q %v", line, err)
	}
	other := strings.TrimSpace(strings.TrimPrefix(line, "LANE "))
	mine, _ := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if mine.Lane() == other {
		t.Fatal("two processes share a lane")
	}
	_ = stdin.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	_ = mine.Close()
	reused, reason := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if reason != AcquireReused {
		t.Fatalf("after both exited a third lane was minted (%s)", reused.Lane())
	}
}
