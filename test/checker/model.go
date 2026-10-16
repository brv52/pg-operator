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

// Package checker is a black-box correctness checker for a PostgreSQL
// endpoint under faults. Concurrent clients run reads, writes and
// compare-and-set operations against a set of integer registers through
// the read-write Service, record every operation's invocation and
// completion, and afterwards:
//
//   - check the history for linearizability with Porcupine, and
//   - reconcile every acknowledged write against the surviving primary to
//     prove no acknowledged write was lost (RPO = 0).
package checker

import (
	"fmt"
	"math"
	"slices"

	"github.com/anishathalye/porcupine"
)

// Kind of register operation.
type Kind string

const (
	KindRead  Kind = "read"
	KindWrite Kind = "write"
	KindCAS   Kind = "cas"
)

// Status of a completed operation from the client's point of view.
type Status string

const (
	// StatusOK: the operation was acknowledged.
	StatusOK Status = "ok"
	// StatusFail: the operation definitely did not take effect (it was
	// rejected, or the request never reached the server).
	StatusFail Status = "fail"
	// StatusInfo: the outcome is unknown. Typical cases are a connection
	// dropped during COMMIT, a timeout while waiting for synchronous
	// replication, or a commit that returned with the "canceling wait for
	// synchronous replication" warning (committed locally, not replicated).
	StatusInfo Status = "info"
)

// Op is one operation in a history.
type Op struct {
	Client int    `json:"client"`
	Kind   Kind   `json:"kind"`
	Key    int64  `json:"key"`
	Value  int64  `json:"value"`          // read result, write value, CAS new value
	From   int64  `json:"from,omitempty"` // CAS expected value
	OK     bool   `json:"ok,omitempty"`   // CAS outcome when acknowledged
	TxID   string `json:"txid,omitempty"`
	Status Status `json:"status"`
	Error  string `json:"error,omitempty"`
	Call   int64  `json:"call"`   // unix nanoseconds
	Return int64  `json:"return"` // unix nanoseconds
}

type input struct {
	kind  Kind
	key   int64
	value int64
	from  int64
}

type output struct {
	value   int64
	ok      bool
	unknown bool
}

// registerModel is a per-key integer register with compare-and-set. A key
// that was never written reads as 0.
var registerModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[int64][]porcupine.Operation{}
		var keys []int64
		for _, op := range history {
			k := op.Input.(input).key
			if _, ok := byKey[k]; !ok {
				keys = append(keys, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		slices.Sort(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() any { return int64(0) },
	Step: func(state, in, out any) (bool, any) {
		s := state.(int64)
		i, o := in.(input), out.(output)
		switch i.kind {
		case KindRead:
			return o.value == s, s
		case KindWrite:
			return true, i.value
		case KindCAS:
			if o.unknown {
				// Indeterminate: it applied iff the expected value matched
				// at the point where it linearizes.
				if s == i.from {
					return true, i.value
				}
				return true, s
			}
			if o.ok {
				return s == i.from, i.value
			}
			return s != i.from, s
		}
		return false, s
	},
	DescribeOperation: func(in, out any) string {
		i, o := in.(input), out.(output)
		suffix := ""
		if o.unknown {
			suffix = " ?"
		}
		switch i.kind {
		case KindRead:
			return fmt.Sprintf("r(%d) -> %d", i.key, o.value)
		case KindWrite:
			return fmt.Sprintf("w(%d, %d)%s", i.key, i.value, suffix)
		default:
			if o.unknown {
				return fmt.Sprintf("cas(%d, %d->%d) ?", i.key, i.from, i.value)
			}
			return fmt.Sprintf("cas(%d, %d->%d) -> %t", i.key, i.from, i.value, o.ok)
		}
	},
	DescribeState: func(state any) string { return fmt.Sprint(state.(int64)) },
}

// toPorcupine converts a history. Failed operations had no effect and are
// dropped, as are reads that did not complete. Indeterminate writes and
// CAS get an infinite return time: they may linearize at any point after
// their invocation, including after every other operation, where their
// effect is invisible. That encodes "may or may not have happened".
func toPorcupine(history []Op) []porcupine.Operation {
	out := make([]porcupine.Operation, 0, len(history))
	for _, op := range history {
		if op.Status == StatusFail || (op.Kind == KindRead && op.Status != StatusOK) {
			continue
		}
		in := input{kind: op.Kind, key: op.Key, value: op.Value, from: op.From}
		o := output{value: op.Value, ok: op.OK}
		ret := op.Return
		if op.Status == StatusInfo {
			o.unknown = true
			ret = math.MaxInt64
		}
		out = append(out, porcupine.Operation{ClientId: op.Client, Input: in, Output: o, Call: op.Call, Return: ret})
	}
	return out
}
