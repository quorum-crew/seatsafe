// Package metrics is a small Prometheus text-format registry: labelled
// counters and histograms, enough for SeatSafe's dashboards without pulling
// in prometheus/client_golang. Output is sorted, so scrapes are stable.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Registry holds metrics in registration order.
type Registry struct {
	mu      sync.Mutex
	metrics []writer
}

type writer interface{ write(w io.Writer) }

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) add(m writer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics = append(r.metrics, m)
}

// Write writes every metric in Prometheus text format (version 0.0.4).
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	ms := append([]writer(nil), r.metrics...)
	r.mu.Unlock()
	for _, m := range ms {
		m.write(w)
	}
}

type desc struct {
	name, help string
	labels     []string
}

func (d desc) header(w io.Writer, typ string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", d.name, d.help, d.name, typ)
}

// key joins label values; \xff never appears in valid UTF-8 label values.
func (d desc) key(values []string) string {
	if len(values) != len(d.labels) {
		panic(fmt.Sprintf("metrics: %s wants %d label values, got %d", d.name, len(d.labels), len(values)))
	}
	return strings.Join(values, "\xff")
}

// labelString renders {a="x",b="y"} plus any extra pair (for le).
func (d desc) labelString(key string, extra ...string) string {
	var parts []string
	if len(d.labels) > 0 {
		for i, v := range strings.Split(key, "\xff") {
			parts = append(parts, d.labels[i]+"="+strconv.Quote(v))
		}
	}
	for i := 0; i+1 < len(extra); i += 2 {
		parts = append(parts, extra[i]+"="+strconv.Quote(extra[i+1]))
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func formatFloat(v float64) string {
	if math.IsInf(v, +1) {
		return "+Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// CounterVec is a counter with labels.
type CounterVec struct {
	desc
	mu   sync.Mutex
	vals map[string]float64
}

// NewCounterVec registers a counter.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{desc: desc{name, help, labels}, vals: map[string]float64{}}
	r.add(c)
	return c
}

// Inc adds 1 to the series with these label values.
func (c *CounterVec) Inc(values ...string) {
	k := c.key(values)
	c.mu.Lock()
	c.vals[k]++
	c.mu.Unlock()
}

// Value returns the current value of one series (for tests).
func (c *CounterVec) Value(values ...string) float64 {
	k := c.key(values)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.vals[k]
}

func (c *CounterVec) write(w io.Writer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.header(w, "counter")
	for _, k := range sortedKeys(c.vals) {
		fmt.Fprintf(w, "%s%s %s\n", c.name, c.labelString(k), formatFloat(c.vals[k]))
	}
}

// HistogramVec is a histogram with labels and fixed buckets.
type HistogramVec struct {
	desc
	buckets []float64 // upper bounds, ascending, without +Inf
	mu      sync.Mutex
	series  map[string]*histSeries
}

type histSeries struct {
	counts []uint64 // per bucket, not cumulative; last is +Inf
	sum    float64
	count  uint64
}

// NewHistogramVec registers a histogram. buckets must be ascending.
func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if !sort.Float64sAreSorted(buckets) {
		panic("metrics: buckets must be ascending")
	}
	h := &HistogramVec{desc: desc{name, help, labels}, buckets: append([]float64(nil), buckets...), series: map[string]*histSeries{}}
	r.add(h)
	return h
}

// Observe records one value.
func (h *HistogramVec) Observe(v float64, values ...string) {
	k := h.key(values)
	i := sort.SearchFloat64s(h.buckets, v) // first bucket with bound >= v
	h.mu.Lock()
	s := h.series[k]
	if s == nil {
		s = &histSeries{counts: make([]uint64, len(h.buckets)+1)}
		h.series[k] = s
	}
	s.counts[i]++
	s.sum += v
	s.count++
	h.mu.Unlock()
}

// Count returns how many values one series has recorded (for tests).
func (h *HistogramVec) Count(values ...string) uint64 {
	k := h.key(values)
	h.mu.Lock()
	defer h.mu.Unlock()
	if s := h.series[k]; s != nil {
		return s.count
	}
	return 0
}

// Sum returns the sum of one series' values (for tests).
func (h *HistogramVec) Sum(values ...string) float64 {
	k := h.key(values)
	h.mu.Lock()
	defer h.mu.Unlock()
	if s := h.series[k]; s != nil {
		return s.sum
	}
	return 0
}

func (h *HistogramVec) write(w io.Writer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.header(w, "histogram")
	for _, k := range sortedKeys(h.series) {
		s := h.series[k]
		var cum uint64
		for i, n := range s.counts {
			cum += n
			le := math.Inf(+1)
			if i < len(h.buckets) {
				le = h.buckets[i]
			}
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, h.labelString(k, "le", formatFloat(le)), cum)
		}
		fmt.Fprintf(w, "%s_sum%s %s\n", h.name, h.labelString(k), formatFloat(s.sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.name, h.labelString(k), s.count)
	}
}
