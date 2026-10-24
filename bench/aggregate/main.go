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

// Command aggregate turns the per-fault results written by
// bench/run_scenario.sh into metrics.json.
//
//	go run ./bench/aggregate -results bench/results -out metrics.json
//
// Every directory under -results is one operator/scenario series
// (pgop-podkill, cnpg-partition, ...). The headline latency is the
// client-observed write unavailability: the checker's widest gap between
// two acknowledged writes during the run, which is the same measurement
// for any operator. Faults that did not inject are reported but left out
// of every statistic.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"
)

type result struct {
	Operator        string   `json:"operator"`
	Scenario        string   `json:"scenario"`
	Injected        bool     `json:"injected"`
	CheckerStatus   string   `json:"checkerStatus"`
	Recovered       bool     `json:"recovered"`
	RecoverySeconds float64  `json:"recoverySeconds"`
	Checker         *checker `json:"checker"`
}

type checker struct {
	Ops             int     `json:"ops"`
	AckedWrites     int     `json:"ackedWrites"`
	Linearizability string  `json:"linearizability"`
	LostWrites      int     `json:"lostWrites"`
	PhantomWrites   int     `json:"phantomWrites"`
	Passed          bool    `json:"passed"`
	MaxWriteGapMs   float64 `json:"maxWriteGapMs"`
}

// Dist is a nearest-rank summary of a sample.
type Dist struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

// Series summarises one operator/scenario directory.
type Series struct {
	Faults          int     `json:"faults"`
	NotInjected     int     `json:"notInjected"`
	CheckerMissing  int     `json:"checkerMissing"`
	CheckerPassed   int     `json:"checkerPassed"`
	Linearizable    int     `json:"linearizable"`
	NotRecovered    int     `json:"notRecovered"`
	Ops             int     `json:"ops"`
	AckedWrites     int     `json:"ackedWrites"`
	LostWrites      int     `json:"lostWrites"`
	PhantomWrites   int     `json:"phantomWrites"`
	WriteGapMs      *Dist   `json:"writeGapMs"`
	RecoverySeconds *Dist   `json:"recoverySeconds"`
	PassRate        float64 `json:"passRate"`
}

// Metrics is the schema of metrics.json. Values that no run has produced
// yet are null rather than zero.
type Metrics struct {
	Generated string `json:"generated"`
	Unit      string `json:"unit"`
	// OpP99 is pg-operator's p99 write unavailability under primary pod
	// kills, OpCNPGP99 the same for CloudNativePG.
	OpP99     *float64 `json:"op_p99"`
	OpCNPGP99 *float64 `json:"op_cnpg_p99"`
	// OpFaults counts injected faults against pg-operator that the
	// checker verified.
	OpFaults int `json:"op_faults"`
	// OpUpgrade is the result of the Kubernetes upgrade drill, copied
	// from bench/upgrade-drill/result.json when present.
	OpUpgrade    json.RawMessage    `json:"op_upgrade"`
	PartitionP99 *float64           `json:"partition_p99"`
	LostWrites   *int               `json:"lost_writes"`
	Porcupine    *string            `json:"porcupine"`
	Series       map[string]*Series `json:"series"`
}

func main() {
	dir := flag.String("results", "bench/results", "directory of operator-scenario result directories")
	out := flag.String("out", "metrics.json", "file to write")
	upgrade := flag.String("upgrade", "bench/upgrade-drill/result.json", "upgrade drill result, if any")
	flag.Parse()

	m, err := aggregate(*dir, *upgrade, time.Now())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	b, _ := json.MarshalIndent(m, "", "  ")
	if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, n := range slices.Sorted(maps.Keys(m.Series)) {
		s := m.Series[n]
		gap := "n/a"
		if s.WriteGapMs != nil {
			gap = fmt.Sprintf("p50 %.0f p99 %.0f max %.0f ms", s.WriteGapMs.P50, s.WriteGapMs.P99, s.WriteGapMs.Max)
		}
		fmt.Printf("%-18s faults %3d  passed %3d  lost %d  gap %s\n", n, s.Faults, s.CheckerPassed, s.LostWrites, gap)
	}
}

func aggregate(dir, upgradeFile string, now time.Time) (*Metrics, error) {
	m := &Metrics{
		Generated: now.UTC().Format(time.RFC3339),
		Unit:      "client-observed write unavailability in milliseconds",
		Series:    map[string]*Series{},
		OpUpgrade: json.RawMessage("null"),
	}
	files, err := filepath.Glob(filepath.Join(dir, "*", "*.json"))
	if err != nil {
		return nil, err
	}
	gaps := map[string][]float64{}
	recovery := map[string][]float64{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var r result
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		key := r.Operator + "-" + r.Scenario
		s := m.Series[key]
		if s == nil {
			s = &Series{}
			m.Series[key] = s
		}
		if !r.Injected {
			s.NotInjected++
			continue
		}
		s.Faults++
		if !r.Recovered {
			s.NotRecovered++
		}
		recovery[key] = append(recovery[key], r.RecoverySeconds)
		c := r.Checker
		if c == nil {
			s.CheckerMissing++
			continue
		}
		s.Ops += c.Ops
		s.AckedWrites += c.AckedWrites
		s.LostWrites += c.LostWrites
		s.PhantomWrites += c.PhantomWrites
		if c.Linearizability == "Ok" {
			s.Linearizable++
		}
		if c.Passed {
			s.CheckerPassed++
		}
		gaps[key] = append(gaps[key], c.MaxWriteGapMs)
	}

	var lost int
	allOK, checked := true, 0
	for key, s := range m.Series {
		s.WriteGapMs = dist(gaps[key])
		s.RecoverySeconds = dist(recovery[key])
		if s.Faults > 0 {
			s.PassRate = math.Round(1000*float64(s.CheckerPassed)/float64(s.Faults)) / 1000
		}
		lost += s.LostWrites
		checked += s.Faults - s.CheckerMissing
		if s.Linearizable != s.Faults-s.CheckerMissing || s.CheckerMissing > 0 {
			allOK = false
		}
		if len(key) > 5 && key[:5] == "pgop-" {
			m.OpFaults += s.CheckerPassed
		}
	}
	m.OpP99 = p99(m.Series["pgop-podkill"])
	m.OpCNPGP99 = p99(m.Series["cnpg-podkill"])
	m.PartitionP99 = p99(m.Series["pgop-partition"])
	if checked > 0 {
		m.LostWrites = &lost
		verdict := "Ok"
		if !allOK {
			verdict = "violations or missing checker results; see series"
		}
		m.Porcupine = &verdict
	}
	if b, err := os.ReadFile(upgradeFile); err == nil && json.Valid(b) {
		m.OpUpgrade = json.RawMessage(b)
	}
	return m, nil
}

func p99(s *Series) *float64 {
	if s == nil || s.WriteGapMs == nil {
		return nil
	}
	v := s.WriteGapMs.P99
	return &v
}

func dist(xs []float64) *Dist {
	if len(xs) == 0 {
		return nil
	}
	xs = slices.Clone(xs)
	slices.Sort(xs)
	return &Dist{P50: rank(xs, 0.50), P95: rank(xs, 0.95), P99: rank(xs, 0.99), Max: xs[len(xs)-1]}
}

// rank is the nearest-rank percentile of a sorted sample.
func rank(sorted []float64, p float64) float64 {
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[max(i, 0)]
}
