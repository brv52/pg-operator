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
	"slices"
	"strings"
	"testing"
)

func TestSyncStandbyNames(t *testing.T) {
	for _, tt := range []struct {
		method string
		n      int
		names  []string
		want   string
	}{
		{"any", 1, []string{"db-3", "db-2"}, `ANY 1 ("db-2", "db-3")`},
		{"first", 2, []string{"db-1", "db-2", "db-3"}, `FIRST 2 ("db-1", "db-2", "db-3")`},
		{"any", 0, []string{"db-2"}, ""},
		{"any", 1, nil, ""},
		{"any", 1, []string{`we"ird`}, `ANY 1 ("we""ird")`},
	} {
		if got := SyncStandbyNames(tt.method, tt.n, tt.names); got != tt.want {
			t.Errorf("SyncStandbyNames(%s, %d, %v) = %q, want %q", tt.method, tt.n, tt.names, got, tt.want)
		}
	}
}

func TestSlotName(t *testing.T) {
	for in, want := range map[string]string{
		"db-1":          "pgop_db_1",
		"Orders.Main-2": "pgop_orders_main_2",
	} {
		if got := SlotName(in); got != want {
			t.Errorf("SlotName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPrimaryConninfoHasNoPassword(t *testing.T) {
	ci := PrimaryConninfo("db-1.db-instances", "db-2")
	if strings.Contains(ci, "password=") {
		t.Fatalf("conninfo must not inline the password: %s", ci)
	}
	for _, want := range []string{"host=db-1.db-instances", "application_name=db-2", "user=replicator", "passfile="} {
		if !strings.Contains(ci, want) {
			t.Errorf("conninfo %q missing %q", ci, want)
		}
	}
}

func TestStaticFlagsLockDurabilitySettings(t *testing.T) {
	flags := StaticFlags()
	for _, want := range []string{"synchronous_commit=on", "wal_log_hints=on", "recovery_target_timeline=latest"} {
		if !slices.Contains(flags, want) {
			t.Errorf("static flags missing %s", want)
		}
	}
	// synchronous_standby_names must stay dynamic: a -c flag would make
	// ALTER SYSTEM ineffective and freeze the quorum membership.
	for _, f := range flags {
		if strings.HasPrefix(f, "synchronous_standby_names") || strings.HasPrefix(f, "primary_conninfo") {
			t.Errorf("dynamic setting rendered as static flag: %s", f)
		}
	}
}

func TestQuoteLiteral(t *testing.T) {
	if got := QuoteLiteral(`it's`); got != `'it''s'` {
		t.Errorf("QuoteLiteral = %s", got)
	}
}
