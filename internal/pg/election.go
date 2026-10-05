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
	"fmt"
	"slices"
)

// Candidate is a standby considered for promotion.
type Candidate struct {
	Name    string
	Ordinal int
	// Reachable is true when the operator could query the instance during
	// this election round.
	Reachable bool
	// InRecovery must be true: an instance that is already a primary is
	// never a candidate (that is the old primary or a split brain).
	InRecovery bool
	Timeline   int64
	ReceiveLSN LSN
	ReplayLSN  LSN
}

// ErrNoQuorum means too few standbys answered to be sure that the most
// advanced one is among them.
var ErrNoQuorum = errors.New("not enough reachable standbys to guarantee no acknowledged write is lost")

// RequiredVoters returns how many of the standbys must be inspected before
// a promotion is safe.
//
// With ANY n (s1..sm), every acknowledged commit was flushed by at least n
// of the m standbys. Any set of m-n+1 standbys intersects every set of n
// standbys, so the highest flushed LSN among m-n+1 reachable standbys is
// at or past every acknowledged commit. With fewer, the only standbys that
// hold the latest commit may be exactly the unreachable ones.
//
// Without synchronous replication (n=0) nothing can be guaranteed and one
// reachable standby is enough to make progress.
func RequiredVoters(standbys, syncNumber int) int {
	if syncNumber <= 0 {
		return 1
	}
	return standbys - syncNumber + 1
}

// Elect picks the standby to promote: the one with the highest timeline,
// then the highest received (flushed) LSN, then the highest replayed LSN,
// then the lowest ordinal so the choice is deterministic across operator
// restarts.
//
// standbys is the number of instances that are configured as standbys
// (reachable or not) and syncNumber the n in ANY n.
func Elect(cands []Candidate, standbys, syncNumber int) (Candidate, error) {
	var voters []Candidate
	for _, c := range cands {
		if c.Reachable && c.InRecovery {
			voters = append(voters, c)
		}
	}
	if need := RequiredVoters(standbys, syncNumber); len(voters) < need {
		return Candidate{}, fmt.Errorf("%w: have %d of %d required", ErrNoQuorum, len(voters), need)
	}
	slices.SortFunc(voters, func(a, b Candidate) int {
		switch {
		case a.Timeline != b.Timeline:
			return cmpDesc(a.Timeline, b.Timeline)
		case a.ReceiveLSN != b.ReceiveLSN:
			return cmpDesc(a.ReceiveLSN, b.ReceiveLSN)
		case a.ReplayLSN != b.ReplayLSN:
			return cmpDesc(a.ReplayLSN, b.ReplayLSN)
		default:
			return a.Ordinal - b.Ordinal
		}
	})
	return voters[0], nil
}

func cmpDesc[T int64 | LSN](a, b T) int {
	if a > b {
		return -1
	}
	return 1
}
