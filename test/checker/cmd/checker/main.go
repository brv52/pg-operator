/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Command checker runs the register workload against a PostgreSQL
// endpoint, then checks the history for linearizability and reconciles
// acknowledged writes against the surviving primary.
//
// It prints a single JSON result line on stdout and exits 0 when the
// history is linearizable and no acknowledged write was lost, 1 when a
// violation was found and 2 when the run itself failed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/brv52/pg-operator/test/checker"
)

func main() {
	var cfg checker.Config
	var out string
	var checkTimeout, settle time.Duration
	flag.StringVar(&cfg.DSN, "dsn", os.Getenv("CHECKER_DSN"), "read-write endpoint (libpq DSN); defaults to $CHECKER_DSN")
	flag.IntVar(&cfg.Workers, "workers", 4, "concurrent clients")
	flag.IntVar(&cfg.Keys, "keys", 4, "registers per run")
	flag.Int64Var(&cfg.KeyBase, "key-base", time.Now().Unix()*100, "first register key; use a fresh base per run")
	flag.DurationVar(&cfg.Duration, "duration", 30*time.Second, "workload duration")
	flag.DurationVar(&cfg.OpTimeout, "op-timeout", 5*time.Second, "timeout of one operation")
	flag.DurationVar(&cfg.Think, "think", 10*time.Millisecond, "pause between operations of one client")
	flag.Uint64Var(&cfg.Seed, "seed", uint64(time.Now().UnixNano()), "operation mix seed")
	flag.StringVar(&out, "out", "", "directory for history.jsonl, journal.jsonl, history.edn and result.json")
	flag.DurationVar(&checkTimeout, "check-timeout", 2*time.Minute, "porcupine time budget")
	flag.DurationVar(&settle, "settle-timeout", 3*time.Minute, "how long to wait for a writable primary at the end")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if out != "" {
		if err := os.MkdirAll(out, 0o755); err != nil {
			fatal(err)
		}
		cfg.JournalPath = filepath.Join(out, "journal.jsonl")
	}

	sctx, cancel := context.WithTimeout(ctx, settle)
	err := checker.Setup(sctx, cfg)
	cancel()
	if err != nil {
		fatal(err)
	}

	start := time.Now()
	history, err := checker.Run(ctx, cfg)
	if err != nil {
		fatal(err)
	}

	fctx, cancel := context.WithTimeout(ctx, settle)
	finalReads, found, err := checker.FinalState(fctx, cfg, cfg.Workers)
	cancel()
	if err != nil {
		fatal(err)
	}
	history = append(history, finalReads...)

	// The history goes to disk before the checks, so a checker that dies
	// while checking still leaves something to analyse.
	visualize := ""
	if out != "" {
		writeJSONL(filepath.Join(out, "history.jsonl"), history)
		visualize = filepath.Join(out, "linearizability.html")
	}
	res := checker.Summarize(history)
	res.Linearizability = checker.CheckLinearizable(history, checkTimeout, visualize)
	lost, phantom := checker.Reconcile(history, found)
	res.LostWrites, res.LostTxIDs, res.PhantomWrites = len(lost), lost, phantom
	res.MaxWriteGap, res.Gaps = checker.WriteGaps(history, 500*time.Millisecond)

	if out != "" {
		if err := checker.WriteEDN(history, filepath.Join(out, "history.edn")); err != nil {
			fmt.Fprintln(os.Stderr, "edn export:", err)
		}
	}

	summary := struct {
		checker.Result
		Passed  bool    `json:"passed"`
		Started string  `json:"started"`
		Seconds float64 `json:"seconds"`
		KeyBase int64   `json:"keyBase"`
		GapMs   float64 `json:"maxWriteGapMs"`
	}{res, res.Passed(), start.UTC().Format(time.RFC3339Nano), time.Since(start).Seconds(), cfg.KeyBase,
		float64(res.MaxWriteGap.Width.Microseconds()) / 1000}
	b, _ := json.Marshal(summary)
	fmt.Println(string(b))
	if out != "" {
		_ = os.WriteFile(filepath.Join(out, "result.json"), append(b, '\n'), 0o644)
	}
	if !res.Passed() {
		os.Exit(1)
	}
}

func writeJSONL(path string, history []checker.Op) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	for _, op := range history {
		_ = enc.Encode(op)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "checker:", err)
	os.Exit(2)
}
