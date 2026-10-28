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
	"slices"
	"time"

	"github.com/anishathalye/porcupine"
)

// Gap is a period with no acknowledged write from any client.
type Gap struct {
	From  time.Time     `json:"from"`
	To    time.Time     `json:"to"`
	Width time.Duration `json:"width"`
}

// Result summarises one checked history.
type Result struct {
	Ops           int `json:"ops"`
	OK            int `json:"ok"`
	Failed        int `json:"failed"`
	Indeterminate int `json:"indeterminate"`
	AckedWrites   int `json:"ackedWrites"`

	// Linearizability is porcupine's verdict: "Ok", "Illegal" or
	// "Unknown" (checker timed out).
	Linearizability porcupine.CheckResult `json:"linearizability"`
	// LostWrites counts acknowledged writes missing from the final state.
	LostWrites int      `json:"lostWrites"`
	LostTxIDs  []string `json:"lostTxIDs,omitempty"`
	// PhantomWrites counts writes present in the final state that the
	// client never attempted (corruption, or a bug in the checker).
	PhantomWrites int `json:"phantomWrites"`

	// MaxWriteGap is the longest interval between two consecutive
	// acknowledged writes: the client-observed write unavailability.
	MaxWriteGap Gap   `json:"maxWriteGap"`
	Gaps        []Gap `json:"gaps,omitempty"`
}

// Passed reports whether the history is linearizable and lost nothing.
func (r Result) Passed() bool {
	return r.Linearizability == porcupine.Ok && r.LostWrites == 0 && r.PhantomWrites == 0
}

// CheckLinearizable runs porcupine over the history. When visualize is not
// empty and the history is not linearizable, an HTML visualization of the
// first key whose sub-history fails is written there.
//
// The verbose check that a visualization needs keeps every partial
// linearization in memory; on the history of an eight-minute run that took
// the checker past 512 MB and the result was lost. So the plain check
// decides, and only one failing key is re-checked verbosely.
func CheckLinearizable(history []Op, timeout time.Duration, visualize string) porcupine.CheckResult {
	ops := toPorcupine(history)
	res := porcupine.CheckOperationsTimeout(registerModel, ops, timeout)
	if res == porcupine.Ok || visualize == "" {
		return res
	}
	for _, part := range registerModel.Partition(ops) {
		if porcupine.CheckOperationsTimeout(registerModel, part, timeout) == porcupine.Ok {
			continue
		}
		_, info := porcupine.CheckOperationsVerbose(registerModel, part, timeout)
		if f, err := os.Create(visualize); err == nil {
			_ = porcupine.Visualize(registerModel, info, f)
			_ = f.Close()
		}
		break
	}
	return res
}

// Reconcile compares the transaction IDs of acknowledged and attempted
// writes with the set found on the surviving primary.
func Reconcile(history []Op, found map[string]bool) (lost []string, phantom int) {
	attempted := map[string]bool{}
	for _, op := range history {
		if op.TxID == "" {
			continue
		}
		attempted[op.TxID] = true
		if op.Status == StatusOK && writes(op) && !found[op.TxID] {
			lost = append(lost, op.TxID)
		}
	}
	for id := range found {
		if !attempted[id] {
			phantom++
		}
	}
	slices.Sort(lost)
	return lost, phantom
}

// writes reports whether an acknowledged op changed state.
func writes(op Op) bool {
	return op.Kind == KindWrite || (op.Kind == KindCAS && op.OK)
}

// WriteGaps returns the gaps between consecutive acknowledged writes that
// are at least min wide, and the widest one.
func WriteGaps(history []Op, minWidth time.Duration) (widest Gap, gaps []Gap) {
	var acks []int64
	for _, op := range history {
		if op.Status == StatusOK && op.Kind != KindRead {
			acks = append(acks, op.Return)
		}
	}
	slices.Sort(acks)
	for i := 1; i < len(acks); i++ {
		g := Gap{From: time.Unix(0, acks[i-1]), To: time.Unix(0, acks[i])}
		g.Width = g.To.Sub(g.From)
		if g.Width > widest.Width {
			widest = g
		}
		if g.Width >= minWidth {
			gaps = append(gaps, g)
		}
	}
	return widest, gaps
}

// Summarize fills the counters of a result.
func Summarize(history []Op) Result {
	r := Result{Ops: len(history)}
	for _, op := range history {
		switch op.Status {
		case StatusOK:
			r.OK++
			if writes(op) {
				r.AckedWrites++
			}
		case StatusFail:
			r.Failed++
		case StatusInfo:
			r.Indeterminate++
		}
	}
	return r
}
