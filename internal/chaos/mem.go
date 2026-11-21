package chaos

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// FreeMem returns the memory, in bytes, available to the cluster: Docker's
// MemAvailable, which on Windows and macOS is that of Docker's VM, where the
// containers run. It is read through the router's container, so the cluster
// must be up.
func (c *Cluster) FreeMem(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	out, err := c.Docker.Run(ctx, "exec", c.Container("router"), "cat", "/proc/meminfo")
	if err != nil {
		return 0, err
	}
	return memAvailable(out)
}

// memAvailable reads MemAvailable from the text of /proc/meminfo.
func memAvailable(meminfo string) (uint64, error) {
	sc := bufio.NewScanner(strings.NewReader(meminfo))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) >= 2 && f[0] == "MemAvailable:" {
			kb, err := strconv.ParseUint(f[1], 10, 64)
			return kb << 10, err
		}
	}
	return 0, errors.New("no MemAvailable in /proc/meminfo")
}

// HostFreeMem returns the host's available physical memory. With Docker in
// a VM this is not what the containers can use, but a host that is nearly
// out of memory slows the VM down, so campaigns record it next to FreeMem.
func HostFreeMem(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	switch runtime.GOOS {
	case "linux":
		b, err := os.ReadFile("/proc/meminfo")
		if err != nil {
			return 0, err
		}
		return memAvailable(string(b))
	case "windows":
		out, err := exec.CommandContext(ctx, "powershell", "-NoProfile", "-NonInteractive", "-Command",
			"(Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory").Output()
		if err != nil {
			return 0, fmt.Errorf("reading the host's free memory: %w", err)
		}
		kb, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
		return kb << 10, err
	}
	return 0, fmt.Errorf("don't know how to read free memory on %s", runtime.GOOS)
}

// waitFreeMem returns the free memory once at least cfg.MinFreeMem is free.
// If it is not, it calls reset once (a fresh cluster holds no Raft logs or
// booked seats in memory) and then polls until cfg.MemWait has passed, and
// returns what is free then.
func waitFreeMem(ctx context.Context, cfg Config, reset func() error) (uint64, error) {
	free, err := cfg.FreeMem(ctx)
	if err != nil || free >= cfg.MinFreeMem {
		return free, err
	}
	if err := reset(); err != nil {
		return free, nil
	}
	deadline := time.Now().Add(cfg.MemWait)
	for {
		if free, err = cfg.FreeMem(ctx); err != nil || free >= cfg.MinFreeMem || time.Now().After(deadline) || ctx.Err() != nil {
			return free, err
		}
		sleep(ctx, 5*time.Second)
	}
}
