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
	"bufio"
	"fmt"
	"os"
	"slices"
)

// WriteEDN exports the history in Jepsen's format for Elle's rw-register
// checker (elle.rw-register), as an independent second opinion on the
// Porcupine verdict:
//
//	lein run -m elle.cli --model rw-register history.edn
//
// Each operation becomes a single-micro-op transaction. CAS has no
// rw-register equivalent: an applied CAS is exported as a write, a failed
// CAS is dropped (Porcupine covers the comparison semantics).
func WriteEDN(history []Op, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	w := bufio.NewWriter(f)

	type event struct {
		at   int64
		line string
	}
	var events []event
	for _, op := range history {
		var mop string
		switch {
		case op.Kind == KindRead && op.Status == StatusOK:
			mop = fmt.Sprintf("[:r %d %d]", op.Key, op.Value)
		case op.Kind == KindRead:
			continue
		case op.Kind == KindWrite || (op.Kind == KindCAS && op.OK) || (op.Kind == KindCAS && op.Status == StatusInfo):
			mop = fmt.Sprintf("[:w %d %d]", op.Key, op.Value)
		default:
			continue
		}
		invoke := mop
		if op.Kind == KindRead {
			invoke = fmt.Sprintf("[:r %d nil]", op.Key)
		}
		typ := map[Status]string{StatusOK: ":ok", StatusFail: ":fail", StatusInfo: ":info"}[op.Status]
		events = append(events,
			event{at: op.Call, line: ednEvent(":invoke", invoke, op.Client, op.Call)},
			event{at: op.Return, line: ednEvent(typ, mop, op.Client, op.Return)},
		)
	}
	slices.SortStableFunc(events, func(a, b event) int {
		switch {
		case a.at < b.at:
			return -1
		case a.at > b.at:
			return 1
		}
		return 0
	})
	for i, e := range events {
		if _, err := fmt.Fprintf(w, "%s\n", e.line[:len(e.line)-1]+fmt.Sprintf(", :index %d}", i)); err != nil {
			return err
		}
	}
	return w.Flush()
}

func ednEvent(typ, value string, process int, at int64) string {
	return fmt.Sprintf("{:type %s, :f :txn, :value [%s], :process %d, :time %d}", typ, value, process, at)
}
