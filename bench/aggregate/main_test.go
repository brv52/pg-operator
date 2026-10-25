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

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRank(t *testing.T) {
	xs := make([]float64, 100)
	for i := range xs {
		xs[i] = float64(i + 1)
	}
	for p, want := range map[float64]float64{0.50: 50, 0.95: 95, 0.99: 99, 1: 100} {
		if got := rank(xs, p); got != want {
			t.Errorf("rank(%v) = %v, want %v", p, got, want)
		}
	}
	if got := rank([]float64{7}, 0.99); got != 7 {
		t.Errorf("single sample: %v", got)
	}
}

func write(t *testing.T, dir, series string, i int, body string) {
	t.Helper()
	d := filepath.Join(dir, series)
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, fmt.Sprintf("%03d.json", i)), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestAggregate(t *testing.T) {
	dir := t.TempDir()
	ok := func(gap float64) string {
		return fmt.Sprintf(`{"operator":"pgop","scenario":"podkill","injected":true,"checkerStatus":"complete",
			"recovered":true,"recoverySeconds":8,"checker":{"ops":100,"ackedWrites":40,"linearizability":"Ok",
			"lostWrites":0,"phantomWrites":0,"passed":true,"maxWriteGapMs":%v}}`, gap)
	}
	write(t, dir, "pgop-podkill", 1, ok(2000))
	write(t, dir, "pgop-podkill", 2, ok(3000))
	write(t, dir, "pgop-podkill", 3, `{"operator":"pgop","scenario":"podkill","injected":false}`)

	m, err := aggregate(dir, filepath.Join(dir, "none.json"), time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	s := m.Series["pgop-podkill"]
	if s.Faults != 2 || s.NotInjected != 1 || s.CheckerPassed != 2 || s.PassRate != 1 {
		t.Fatalf("series = %+v", s)
	}
	if m.OpP99 == nil || *m.OpP99 != 3000 || m.OpFaults == nil || *m.OpFaults != 2 {
		t.Fatalf("op_p99 = %v, op_faults = %v", m.OpP99, m.OpFaults)
	}

	empty, err := aggregate(filepath.Join(dir, "none"), "", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if empty.OpFaults != nil || empty.LostWrites != nil || empty.Porcupine != nil {
		t.Fatal("no runs must aggregate to nulls")
	}
	if m.OpCNPGP99 != nil || m.PartitionP99 != nil {
		t.Fatal("series without runs must stay null")
	}
	if m.LostWrites == nil || *m.LostWrites != 0 || m.Porcupine == nil || *m.Porcupine != "Ok" {
		t.Fatalf("lost = %v, porcupine = %v", m.LostWrites, m.Porcupine)
	}
	if string(m.OpUpgrade) != "null" {
		t.Fatalf("op_upgrade = %s", m.OpUpgrade)
	}
}
