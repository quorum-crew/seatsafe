package metrics

import (
	"strings"
	"testing"
)

func TestTextFormat(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("x_total", "Things.", "kind")
	h := r.NewHistogramVec("y_seconds", "Latency.", []float64{0.1, 1}, "path")
	c.Inc("b")
	c.Inc("a")
	c.Inc("b")
	h.Observe(0.05, "single")
	h.Observe(0.1, "single") // on a bound: counted in that bucket
	h.Observe(5, "single")

	var sb strings.Builder
	r.Write(&sb)
	want := `# HELP x_total Things.
# TYPE x_total counter
x_total{kind="a"} 1
x_total{kind="b"} 2
# HELP y_seconds Latency.
# TYPE y_seconds histogram
y_seconds_bucket{path="single",le="0.1"} 2
y_seconds_bucket{path="single",le="1"} 2
y_seconds_bucket{path="single",le="+Inf"} 3
y_seconds_sum{path="single"} 5.15
y_seconds_count{path="single"} 3
`
	if sb.String() != want {
		t.Fatalf("got\n%s\nwant\n%s", sb.String(), want)
	}
	if c.Value("b") != 2 || c.Value("zzz") != 0 || h.Count("single") != 3 || h.Count("none") != 0 || h.Sum("single") != 5.15 || h.Sum("none") != 0 {
		t.Fatal("accessors disagree with output")
	}
}

func TestNoLabels(t *testing.T) {
	r := NewRegistry()
	r.NewCounterVec("z_total", "Z.").Inc()
	var sb strings.Builder
	r.Write(&sb)
	if !strings.Contains(sb.String(), "\nz_total 1\n") {
		t.Fatalf("got %q", sb.String())
	}
}

func TestPanics(t *testing.T) {
	r := NewRegistry()
	for name, f := range map[string]func(){
		"label count": func() { r.NewCounterVec("a", "a", "x").Inc() },
		"buckets":     func() { r.NewHistogramVec("b", "b", []float64{2, 1}) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic", name)
				}
			}()
			f()
		}()
	}
}

// Init creates a series at 0 without touching one that already counts.
func TestInit(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("c_total", "c", "group")
	h := r.NewHistogramVec("h_seconds", "h", []float64{1}, "group")
	c.Inc("a")
	c.Init("a")
	c.Init("b")
	h.Observe(0.5, "a")
	h.Init("a")
	h.Init("b")
	var b strings.Builder
	r.Write(&b)
	for _, want := range []string{`c_total{group="a"} 1`, `c_total{group="b"} 0`, `h_seconds_count{group="a"} 1`, `h_seconds_count{group="b"} 0`, `h_seconds_bucket{group="b",le="+Inf"} 0`} {
		if !strings.Contains(b.String(), want+"\n") {
			t.Errorf("missing %s in:\n%s", want, b.String())
		}
	}
}
