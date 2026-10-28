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
