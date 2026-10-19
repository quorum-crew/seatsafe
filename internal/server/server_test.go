package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthAndMetrics(t *testing.T) {
	ts := httptest.NewServer(Handler(Info{Role: "storage", NodeID: "shard1-node1", Shard: "1"}))
	defer ts.Close()

	for path, want := range map[string]string{
		"/healthz": "ok role=storage",
		"/metrics": `seatsafe_up{role="storage",node="shard1-node1",shard="1"} 1`,
	} {
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