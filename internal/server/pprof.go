package server

import (
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
)

// Profiling rates set when PPROF=1: a block event is sampled about once per
// 10 µs spent blocked, and one mutex contention event in 100.
const (
	pprofBlockRateNs   = 10_000
	pprofMutexFraction = 100
)

// pprofEnabled reports whether PPROF=1. Profiling is off by default: the
// endpoints expose stacks and the block and mutex profiles cost CPU.
func pprofEnabled() bool { return os.Getenv("PPROF") == "1" }

// mountPprof adds net/http/pprof's routes under /debug/pprof/ and turns on
// block and mutex profiling.
func mountPprof(mux *http.ServeMux) {
	runtime.SetBlockProfileRate(pprofBlockRateNs)
	runtime.SetMutexProfileFraction(pprofMutexFraction)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}
