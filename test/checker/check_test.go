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

package checker

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// h builds a history from compact tuples; times are in milliseconds.
func op(client int, kind Kind, key, value int64, call, ret int64, status Status) Op {
	return Op{Client: client, Kind: kind, Key: key, Value: value, Call: call * 1e6, Return: ret * 1e6, Status: status}
}

func cas(client int, key, from, to int64, ok bool, call, ret int64, status Status) Op {
	o := op(client, KindCAS, key, to, call, ret, status)
	o.From, o.OK = from, ok
	return o
}

func check(t *testing.T, history []Op) porcupine.CheckResult {
	t.Helper()
	return CheckLinearizable(history, 10*time.Second, "")
}

func TestLinearizableHistories(t *testing.T) {
	tests := []struct {
		name    string
		history []Op
		want    porcupine.CheckResult
	}{
		{
			name: "sequential write then read",
			history: []Op{
				op(0, KindWrite, 1, 5, 0, 10, StatusOK),
				op(1, KindRead, 1, 5, 20, 30, StatusOK),
			},
			want: porcupine.Ok,
		},
		{
			name: "stale read after acknowledged write",
			history: []Op{
				op(0, KindWrite, 1, 5, 0, 10, StatusOK),
				op(1, KindRead, 1, 0, 20, 30, StatusOK),
			},
			want: porcupine.Illegal,
		},
		{
			name: "concurrent read may see either value",
			history: []Op{
				op(0, KindWrite, 1, 5, 0, 30, StatusOK),
				op(1, KindRead, 1, 0, 10, 20, StatusOK),
			},
			want: porcupine.Ok,
		},
		{
			name: "lost acknowledged write shows up as a stale read",
			history: []Op{
				op(0, KindWrite, 1, 1, 0, 10, StatusOK),
				op(0, KindWrite, 1, 2, 20, 30, StatusOK),
				op(1, KindRead, 1, 1, 100, 110, StatusOK),
			},
			want: porcupine.Illegal,
		},
		{
			name: "indeterminate write may have happened",
			history: []Op{
				op(0, KindWrite, 1, 7, 0, 10, StatusInfo),
				op(1, KindRead, 1, 7, 50, 60, StatusOK),
			},
			want: porcupine.Ok,
		},
		{
			name: "indeterminate write may not have happened",
			history: []Op{
				op(0, KindWrite, 1, 7, 0, 10, StatusInfo),
				op(1, KindRead, 1, 0, 50, 60, StatusOK),
			},
			want: porcupine.Ok,
		},
		{
			name: "but it cannot happen twice",
			history: []Op{
				op(0, KindWrite, 1, 7, 0, 10, StatusInfo),
				op(1, KindRead, 1, 7, 50, 60, StatusOK),
				op(1, KindWrite, 1, 8, 70, 80, StatusOK),
				op(1, KindRead, 1, 7, 90, 100, StatusOK),
			},
			want: porcupine.Illegal,
		},
		{
			name: "failed write is ignored",
			history: []Op{
				op(0, KindWrite, 1, 9, 0, 10, StatusFail),
				op(1, KindRead, 1, 0, 50, 60, StatusOK),
			},
			want: porcupine.Ok,
		},
		{
			name: "two successful CAS from the same value",
			history: []Op{
				cas(0, 1, 0, 1, true, 0, 10, StatusOK),
				cas(1, 1, 0, 2, true, 20, 30, StatusOK),
			},
			want: porcupine.Illegal,
		},
		{
			name: "failed CAS must observe a different value",
			history: []Op{
				op(0, KindWrite, 1, 3, 0, 10, StatusOK),
				cas(1, 1, 3, 4, false, 20, 30, StatusOK),
			},
			want: porcupine.Illegal,
		},
		{
			name: "keys are independent",
			history: []Op{
				op(0, KindWrite, 1, 5, 0, 10, StatusOK),
				op(1, KindRead, 2, 0, 20, 30, StatusOK),
			},
			want: porcupine.Ok,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := check(t, tt.history); got != tt.want {
				t.Fatalf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestReconcile(t *testing.T) {
	h := []Op{
		{Kind: KindWrite, TxID: "a", Status: StatusOK},
		{Kind: KindWrite, TxID: "b", Status: StatusOK},
		{Kind: KindWrite, TxID: "c", Status: StatusInfo},
		{Kind: KindCAS, TxID: "d", Status: StatusOK, OK: false},
		{Kind: KindWrite, TxID: "e", Status: StatusFail},
	}
	lost, phantom := Reconcile(h, map[string]bool{"a": true, "c": true, "zz": true})
	if len(lost) != 1 || lost[0] != "b" {
		t.Errorf("lost = %v, want [b]", lost)
	}
	if phantom != 1 {
		t.Errorf("phantom = %d, want 1", phantom)
	}
}

func TestWriteGaps(t *testing.T) {
	h := []Op{
		op(0, KindWrite, 1, 1, 0, 10, StatusOK),
		op(0, KindRead, 1, 1, 11, 12, StatusOK),
		op(0, KindWrite, 1, 2, 12, 20, StatusOK),
		op(0, KindWrite, 1, 3, 20, 25, StatusFail),
		op(0, KindWrite, 1, 3, 30, 2020, StatusOK),
	}
	widest, gaps := WriteGaps(h, time.Second)
	if widest.Width != 2000*time.Millisecond || len(gaps) != 1 {
		t.Fatalf("widest=%v gaps=%v", widest.Width, gaps)
	}
}

func TestVisualizesOnlyTheFailingKey(t *testing.T) {
	out := filepath.Join(t.TempDir(), "lin.html")
	history := []Op{
		op(0, KindWrite, 1, 5, 0, 10, StatusOK),
		op(1, KindRead, 1, 5, 20, 30, StatusOK),
		op(0, KindWrite, 2, 7, 0, 10, StatusOK),
		op(1, KindRead, 2, 0, 20, 30, StatusOK),
	}
	if got := CheckLinearizable(history, 10*time.Second, out); got != porcupine.Illegal {
		t.Fatalf("got %v", got)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 {
		t.Fatal("empty visualization")
	}

	ok := filepath.Join(t.TempDir(), "ok.html")
	CheckLinearizable(history[:2], 10*time.Second, ok)
	if _, err := os.Stat(ok); !os.IsNotExist(err) {
		t.Fatal("a linearizable history must not be visualized")
	}
}
