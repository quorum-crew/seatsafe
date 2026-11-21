// Package clusterctl runs SeatSafe on the machines of an inventory
// (internal/inventory): it generates the configuration, copies it to the
// hosts, starts and stops the containers, runs benchmarks with several load
// generators, and runs chaos campaigns. cmd/clusterctl is its command line.
//
// Everything it does on a host is a shell command given to an Executor, so
// the same code drives real machines over ssh, this machine alone (a local
// proof without AWS), a recording fake in tests, and -dry-run.
package clusterctl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/quorum-crew/seatsafe/internal/inventory"
)

// Executor runs commands and moves files on the hosts of an inventory.
type Executor interface {
	// Run runs a shell command on the host and returns its standard output.
	Run(ctx context.Context, h inventory.Host, cmd string) (string, error)
	// Copy copies a local file or directory to a path on the host.
	Copy(ctx context.Context, h inventory.Host, local, remote string) error
	// Fetch copies a file or directory on the host to a local path.
	Fetch(ctx context.Context, h inventory.Host, remote, local string) error
}

// SSH runs commands with the ssh and scp programs.
type SSH struct {
	User, Key string
	// Private makes it connect to the hosts' private addresses, for when it
	// runs inside the cluster's network (on a load generator host).
	Private bool
}

func (s SSH) addr(h inventory.Host) string {
	if s.Private || h.PublicIP == "" {
		return h.PrivateIP
	}
	return h.PublicIP
}

func (s SSH) options() []string {
	return []string{"-i", expandHome(s.Key), "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=10",
		"-o", "ServerAliveInterval=15", "-o", "LogLevel=ERROR"}
}

func (s SSH) Run(ctx context.Context, h inventory.Host, cmd string) (string, error) {
	return run(ctx, nil, "ssh", append(s.options(), s.User+"@"+s.addr(h), "--", cmd)...)
}

func (s SSH) Copy(ctx context.Context, h inventory.Host, local, remote string) error {
	_, err := run(ctx, nil, "scp", append(append([]string{"-r", "-q"}, s.options()...), local, s.User+"@"+s.addr(h)+":"+remote)...)
	return err
}

func (s SSH) Fetch(ctx context.Context, h inventory.Host, remote, local string) error {
	if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
		return err
	}
	_, err := run(ctx, nil, "scp", append(append([]string{"-r", "-q"}, s.options()...), s.User+"@"+s.addr(h)+":"+remote, local)...)
	return err
}

func expandHome(p string) string {
	if strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, p[2:])
		}
	}
	return p
}

// Local runs every command on this machine with sh: all the hosts of the
// inventory are this machine.
type Local struct{}

// localEnv stops Git for Windows from rewriting the container paths in
// docker commands; it does nothing elsewhere.
var localEnv = []string{"MSYS_NO_PATHCONV=1", "MSYS2_ARG_CONV_EXCL=*"}

func (Local) Run(ctx context.Context, _ inventory.Host, cmd string) (string, error) {
	return run(ctx, localEnv, "sh", "-c", cmd)
}

func (Local) Copy(_ context.Context, _ inventory.Host, local, remote string) error {
	return copyTree(local, remote)
}

func (Local) Fetch(_ context.Context, _ inventory.Host, remote, local string) error {
	return copyTree(remote, local)
}

// copyTree copies a file or a directory tree.
func copyTree(from, to string) error {
	return filepath.WalkDir(from, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, p)
		dst := filepath.Join(to, rel)
		if d.IsDir() {
			return os.MkdirAll(dst, 0o755)
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		in, err := os.Open(p)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(dst)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	})
}

func run(ctx context.Context, env []string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if env != nil {
		cmd.Env = append(os.Environ(), env...)
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(errb.String())
		if msg == "" {
			msg = strings.TrimSpace(out.String())
		}
		if len(msg) > 2000 {
			msg = msg[:2000] + "..."
		}
		return out.String(), fmt.Errorf("%s: %w: %s", name, err, msg)
	}
	return out.String(), nil
}

// DryRun prints every command instead of running it. Commands return
// nothing, so anything that would wait for the cluster returns at once.
type DryRun struct {
	W  io.Writer
	mu sync.Mutex
}

func (d *DryRun) print(format string, args ...any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	fmt.Fprintf(d.W, format, args...)
}

func (d *DryRun) Run(_ context.Context, h inventory.Host, cmd string) (string, error) {
	d.print("[%s] %s\n", h.Name, cmd)
	return "", nil
}

func (d *DryRun) Copy(_ context.Context, h inventory.Host, local, remote string) error {
	d.print("[%s] copy %s -> %s\n", h.Name, filepath.ToSlash(local), remote)
	return nil
}

func (d *DryRun) Fetch(_ context.Context, h inventory.Host, remote, local string) error {
	d.print("[%s] fetch %s -> %s\n", h.Name, remote, filepath.ToSlash(local))
	return nil
}

// Call is one thing a Fake was asked to do.
type Call struct {
	Host string
	Cmd  string // the command, or "copy A -> B" / "fetch A -> B"
}

// Fake records what it is asked to do and answers from Reply. It is the
// executor of the tests.
type Fake struct {
	// Reply gives a command's output; nil or ("", nil) for no output.
	Reply func(h inventory.Host, cmd string) (string, error)
	// OnFetch, if set, creates what a Fetch would have copied.
	OnFetch func(h inventory.Host, remote, local string) error
	// OnCopy, if set, is told of every Copy.
	OnCopy func(h inventory.Host, local, remote string)

	mu    sync.Mutex
	calls []Call
}

func (f *Fake) add(h inventory.Host, s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, Call{h.Name, s})
}

func (f *Fake) Run(_ context.Context, h inventory.Host, cmd string) (string, error) {
	f.add(h, cmd)
	if f.Reply == nil {
		return "", nil
	}
	return f.Reply(h, cmd)
}

func (f *Fake) Copy(_ context.Context, h inventory.Host, local, remote string) error {
	f.add(h, "copy "+filepath.ToSlash(local)+" -> "+remote)
	if f.OnCopy != nil {
		f.OnCopy(h, local, remote)
	}
	return nil
}

func (f *Fake) Fetch(_ context.Context, h inventory.Host, remote, local string) error {
	f.add(h, "fetch "+remote+" -> "+filepath.ToSlash(local))
	if f.OnFetch != nil {
		return f.OnFetch(h, remote, local)
	}
	return nil
}

// Calls returns everything recorded so far, in order.
func (f *Fake) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Call(nil), f.calls...)
}

// On returns the commands recorded for one host, in order.
func (f *Fake) On(host string) []string {
	var out []string
	for _, c := range f.Calls() {
		if c.Host == host {
			out = append(out, c.Cmd)
		}
	}
	return out
}

// shq quotes s for the shell.
func shq(s string) string {
	if s != "" && !strings.ContainsAny(s, " \t\n'\"\\$`!*?[]{}()<>|&;~#") {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// each runs f for every host at the same time and returns the first error,
// with the host's name.
func each(hosts []inventory.Host, f func(h inventory.Host) error) error {
	errs := make([]error, len(hosts))
	var wg sync.WaitGroup
	for i, h := range hosts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f(h); err != nil {
				errs[i] = fmt.Errorf("%s: %w", h.Name, err)
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
