// Package loadgen drives BookingService and reports honest throughput, latency
// and correctness numbers.
//
// Open-loop runs send at fixed intended times without waiting for responses,
// and latency is measured from the intended time, so a slow cluster can't hide
// behind a slowed-down client (coordinated omission). Every order can be
// audited afterwards against what the cluster says is available, and every
// call attempt can be written to a history for offline checking.
package loadgen

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
)

// Modes and workloads.
const (
	ModeOpen   = "open"
	ModeClosed = "closed"
	ModeRamp   = "ramp"

	WorkloadUniform = "uniform"
	WorkloadFlash   = "flash"
	WorkloadFresh   = "fresh"
)

// FlashEvent is the event the flash workload books.
const FlashEvent = "evt-flash"

// Config is one load generator run.
type Config struct {
	Target   string `json:"target"`
	Conns    int    `json:"conns"`
	ShardMap string `json:"shardmap"`
	SeedDir  string `json:"seed_dir"`

	Mode     string `json:"mode"`
	Workload string `json:"workload"`

	// Open.
	Rate        float64       `json:"rate"`
	Duration    time.Duration `json:"duration_ns"`
	Warmup      time.Duration `json:"warmup_ns"`
	MaxInflight int           `json:"max_inflight"`

	// Closed.
	Users int           `json:"users"`
	Think time.Duration `json:"think_ns"`

	// Ramp.
	RateStart    float64       `json:"rate_start"`
	RateEnd      float64       `json:"rate_end"`
	RateStep     float64       `json:"rate_step"`
	StepDuration time.Duration `json:"step_duration_ns"`
	SLO          time.Duration `json:"slo_ns"`

	// Orders.
	ZipfS          float64 `json:"zipf_s"`
	OrderSizes     string  `json:"order_sizes"`
	CrossShardFrac float64 `json:"cross_shard_frac"`
	Seed           uint64  `json:"seed"`

	// Client behavior.
	CallTimeout time.Duration `json:"call_timeout_ns"`
	RetryBudget time.Duration `json:"retry_budget_ns"`

	// Output.
	Out       string `json:"out"`
	Label     string `json:"label"`
	History   bool   `json:"history"`
	Audit     bool   `json:"audit"`
	GitCommit string `json:"git_commit"`
	Quiet     bool   `json:"-"`

	// DialOptions are added to every connection (tests use a bufconn dialer).
	DialOptions []grpc.DialOption `json:"-"`
}

// Defaults returns the documented defaults.
func Defaults() Config {
	return Config{
		Target: "localhost:17000", Conns: 16, ShardMap: "deploy/shardmap.json", SeedDir: "deploy/seed",
		Mode: ModeOpen, Workload: WorkloadUniform,
		Rate: 1000, Duration: 60 * time.Second, Warmup: 5 * time.Second, MaxInflight: 20000,
		Users:     1000,
		RateStart: 500, RateEnd: 20000, RateStep: 500, StepDuration: 10 * time.Second, SLO: 100 * time.Millisecond,
		ZipfS: 1.1, OrderSizes: "1:70,2:20,4:10", Seed: 1,
		CallTimeout: 2 * time.Second, RetryBudget: 5 * time.Second,
		Audit: true,
	}
}

// Validate checks the flags that can be checked before connecting.
func (c Config) Validate() error {
	switch c.Mode {
	case ModeOpen:
		if c.Rate <= 0 {
			return errors.New("loadgen: -rate must be positive")
		}
	case ModeClosed:
		if c.Users < 1 {
			return errors.New("loadgen: -users must be positive")
		}
	case ModeRamp:
		if c.RateStart <= 0 || c.RateStep <= 0 || c.RateEnd < c.RateStart || c.StepDuration <= 0 {
			return errors.New("loadgen: ramp needs -rate-start > 0, -rate-step > 0, -rate-end >= -rate-start and -step-duration > 0")
		}
	default:
		return fmt.Errorf("loadgen: unknown -mode %q", c.Mode)
	}
	switch c.Workload {
	case WorkloadUniform, WorkloadFresh:
	case WorkloadFlash:
		if c.ZipfS <= 1 {
			return errors.New("loadgen: -zipf-s must be greater than 1")
		}
	default:
		return fmt.Errorf("loadgen: unknown -workload %q", c.Workload)
	}
	if c.Conns < 1 || c.MaxInflight < 1 {
		return errors.New("loadgen: -conns and -max-inflight must be positive")
	}
	if c.CrossShardFrac < 0 || c.CrossShardFrac > 1 {
		return errors.New("loadgen: -cross-shard-frac must be between 0 and 1")
	}
	if c.Mode != ModeRamp && c.Duration <= 0 {
		return errors.New("loadgen: -duration must be positive")
	}
	_, err := ParseOrderSizes(c.OrderSizes)
	return err
}

// OrderSizes picks a number of seats per order by weight.
type OrderSizes struct {
	sizes   []int
	cumul   []int
	total   int
	largest int
}

// ParseOrderSizes reads "size:weight,..." such as "1:70,2:20,4:10".
func ParseOrderSizes(s string) (OrderSizes, error) {
	var o OrderSizes
	for _, part := range strings.Split(s, ",") {
		size, weight, ok := strings.Cut(strings.TrimSpace(part), ":")
		n, err1 := strconv.Atoi(size)
		w, err2 := strconv.Atoi(weight)
		if !ok || err1 != nil || err2 != nil || n < 1 || w < 1 {
			return o, fmt.Errorf("loadgen: bad -order-sizes entry %q, want size:weight", part)
		}
		o.total += w
		o.sizes, o.cumul = append(o.sizes, n), append(o.cumul, o.total)
		o.largest = max(o.largest, n)
	}
	return o, nil
}

// Pick maps a number in [0, total) to a size.
func (o OrderSizes) pick(v int) int {
	for i, c := range o.cumul {
		if v < c {
			return o.sizes[i]
		}
	}
	return o.sizes[len(o.sizes)-1]
}
