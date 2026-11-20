package server

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
)

func TestHealthAndMetrics(t *testing.T) {
	extra := func(w io.Writer) { fmt.Fprintln(w, "extra_metric 7") }
	ts := httptest.NewServer(Handler(Info{Role: "storage", NodeID: "shard1-node1", Shard: "1"}, extra))
	defer ts.Close()

	for path, want := range map[string]string{
		"/healthz":  "ok role=storage",
		"/metrics":  `seatsafe_up{role="storage",node="shard1-node1",shard="1"} 1`,
		"/metrics#": "\nextra_metric 7\n",
	} {
		path = strings.TrimSuffix(path, "#")
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), want) {
			t.Errorf("%s: status=%d body=%q, want substring %q", path, resp.StatusCode, body, want)
		}
	}
}

func TestPprofOnlyWithPprofEnv(t *testing.T) {
	get := func(ts *httptest.Server) int {
		resp, err := http.Get(ts.URL + "/debug/pprof/")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	t.Setenv("PPROF", "")
	off := httptest.NewServer(Handler(Info{Role: "router"}))
	defer off.Close()
	if code := get(off); code != http.StatusNotFound {
		t.Fatalf("/debug/pprof/ without PPROF: status %d, want 404", code)
	}

	t.Setenv("PPROF", "1")
	t.Cleanup(func() { runtime.SetBlockProfileRate(0); runtime.SetMutexProfileFraction(0) })
	on := httptest.NewServer(Handler(Info{Role: "router"}))
	defer on.Close()
	if code := get(on); code != http.StatusOK {
		t.Fatalf("/debug/pprof/ with PPROF=1: status %d, want 200", code)
	}
}
