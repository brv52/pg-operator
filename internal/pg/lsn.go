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

// Package pg holds everything the operator knows about PostgreSQL itself:
// the SQL it runs against instances, how replication settings are
// rendered, how a promotion candidate is chosen and the shell scripts that
// bootstrap, clone and rewind an instance inside its pod.
package pg

import (
	"fmt"
	"strconv"
	"strings"
)

// LSN is a PostgreSQL write-ahead log location.
type LSN uint64

// InvalidLSN is the zero location, used for "unknown" (e.g. a standby that
// has not received any WAL yet).
const InvalidLSN LSN = 0

// ParseLSN parses the textual "XXXXXXXX/YYYYYYYY" form returned by
// pg_current_wal_lsn() and friends.
func ParseLSN(s string) (LSN, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return InvalidLSN, fmt.Errorf("invalid LSN %q: missing '/'", s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return InvalidLSN, fmt.Errorf("invalid LSN %q: %w", s, err)
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return InvalidLSN, fmt.Errorf("invalid LSN %q: %w", s, err)
	}
	return LSN(h<<32 | l), nil
}

func (l LSN) String() string {
	return fmt.Sprintf("%X/%X", uint64(l)>>32, uint64(l)&0xFFFFFFFF)
}
