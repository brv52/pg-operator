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

package pg

import (
	"errors"
	"math/rand/v2"
	"testing"
)

func TestParseLSN(t *testing.T) {
	for in, want := range map[string]LSN{
		"0/0":               0,
		"0/3000060":         0x3000060,
		"16/B374D848":       0x16B374D848,
		"FFFFFFFF/FFFFFFFF": 0xFFFFFFFFFFFFFFFF,
	} {
		got, err := ParseLSN(in)
		if err != nil || got != want {
			t.Errorf("ParseLSN(%q) = %v, %v; want %v", in, got, err, want)
		}
		if got.String() != in {
			t.Errorf("round trip %q -> %q", in, got.String())
		}
	}
	for _, bad := range []string{"", "0", "x/1", "1/", "100000000/0"} {
		if _, err := ParseLSN(bad); err == nil {
			t.Errorf("ParseLSN(%q) succeeded", bad)
		}
	}
}

func TestRequiredVoters(t *testing.T) {
	for _, tt := range []struct{ standbys, n, want int }{
		{2, 1, 2}, // 3 instances, ANY 1: both standbys must answer
		{4, 1, 4},
		{4, 2, 3},
		{4, 4, 1}, // every standby is synchronous: any one has everything
		{2, 0, 1}, // async
	} {
		if got := RequiredVoters(tt.standbys, tt.n); got != tt.want {
			t.Errorf("RequiredVoters(%d, %d) = %d, want %d", tt.standbys, tt.n, got, tt.want)
		}
	}
}

func TestElect(t *testing.T) {
	c := func(name string, ord int, tli int64, recv, replay LSN) Candidate {
		return Candidate{Name: name, Ordinal: ord, Reachable: true, InRecovery: true, Timeline: tli, ReceiveLSN: recv, ReplayLSN: replay}
	}

	tests := []struct {
		name    string
		cands   []Candidate
		n       int
		want    string
		wantErr error
	}{
		{
			name:  "highest received LSN wins over replay",
			cands: []Candidate{c("db-2", 2, 1, 100, 100), c("db-3", 3, 1, 200, 50)},
			n:     1, want: "db-3",
		},
		{
			name:  "newer timeline wins",
			cands: []Candidate{c("db-2", 2, 2, 100, 100), c("db-3", 3, 1, 900, 900)},
			n:     1, want: "db-2",
		},
		{
			name:  "replay breaks receive tie",
			cands: []Candidate{c("db-2", 2, 1, 100, 10), c("db-3", 3, 1, 100, 90)},
			n:     1, want: "db-3",
		},
		{
			name:  "ordinal breaks full tie",
			cands: []Candidate{c("db-3", 3, 1, 100, 100), c("db-2", 2, 1, 100, 100)},
			n:     1, want: "db-2",
		},
		{
			name: "unreachable standby blocks ANY 1 of 2",
			cands: []Candidate{c("db-2", 2, 1, 100, 100),
				{Name: "db-3", Ordinal: 3, Reachable: false}},
			n: 1, wantErr: ErrNoQuorum,
		},
		{
			name: "instance out of recovery is not a voter",
			cands: []Candidate{c("db-2", 2, 1, 100, 100),
				{Name: "db-3", Ordinal: 3, Reachable: true, InRecovery: false, ReceiveLSN: 500}},
			n: 1, wantErr: ErrNoQuorum,
		},
		{
			name:  "async needs one voter",
			cands: []Candidate{c("db-2", 2, 1, 100, 100), {Name: "db-3", Ordinal: 3}},
			n:     0, want: "db-2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Elect(tt.cands, len(tt.cands), tt.n)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil || got.Name != tt.want {
				t.Fatalf("Elect = %q, %v; want %q", got.Name, err, tt.want)
			}
		})
	}
}

// TestElectNeverLosesAcknowledgedCommit simulates random ANY n replication
// histories and checks the core safety property: whatever subset of
// standbys answers, if Elect returns a winner, that winner has flushed
// every commit the primary acknowledged.
func TestElectNeverLosesAcknowledgedCommit(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 20000 {
		m := 1 + rng.IntN(6)
		n := 1 + rng.IntN(m)

		flushed := make([]LSN, m)
		var acked LSN
		for step := range 30 {
			// The primary writes a commit at the next LSN and some random
			// subset of standbys flushes everything up to it.
			commit := LSN(step + 1)
			got := 0
			for i := range flushed {
				if rng.IntN(2) == 0 {
					flushed[i] = commit
				}
				if flushed[i] >= commit {
					got++
				}
			}
			if got >= n {
				acked = commit
			}
		}

		cands := make([]Candidate, m)
		for i := range cands {
			cands[i] = Candidate{
				Name: string(rune('a' + i)), Ordinal: i, InRecovery: true,
				Reachable: rng.IntN(3) != 0, Timeline: 1, ReceiveLSN: flushed[i],
			}
		}
		w, err := Elect(cands, m, n)
		if errors.Is(err, ErrNoQuorum) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if w.ReceiveLSN < acked {
			t.Fatalf("m=%d n=%d: elected %s at %v but %v was acknowledged (cands %+v)", m, n, w.Name, w.ReceiveLSN, acked, cands)
		}
	}
}
