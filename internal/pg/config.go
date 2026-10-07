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
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	// Port every instance listens on.
	Port = 5432
	// SuperUser is the bootstrap superuser the operator connects as.
	SuperUser = "postgres"
	// ReplicationUser streams WAL between instances.
	ReplicationUser = "replicator"

	// SlotPrefix marks replication slots owned by the operator. Slots
	// without it are never dropped.
	SlotPrefix = "pgop_"
)

// Timeouts that bound how long a dead peer stays "connected". With the
// PostgreSQL defaults (60s) a standby whose primary is partitioned away
// keeps reporting status=streaming for a full minute, which would hide a
// partition from the operator's standby-side detector.
const (
	WalSenderTimeout   = 5 * time.Second
	WalReceiverTimeout = 5 * time.Second
)

// StaticParameters are passed as -c flags on the postgres command line.
// Command-line settings take precedence over postgresql.conf and
// postgresql.auto.conf, so nothing (including ALTER SYSTEM run by hand)
// can weaken them on a running instance.
//
// Settings that change at runtime (synchronous_standby_names,
// primary_conninfo, primary_slot_name) are deliberately absent: the
// operator converges those with ALTER SYSTEM, which a -c flag would mask.
func StaticParameters() map[string]string {
	return map[string]string{
		"listen_addresses":             "*",
		"port":                         fmt.Sprint(Port),
		"unix_socket_directories":      "/tmp",
		"hba_file":                     "/etc/pg/pg_hba.conf",
		"wal_level":                    "replica",
		"hot_standby":                  "on",
		"hot_standby_feedback":         "on",
		"wal_log_hints":                "on",
		"max_wal_senders":              "10",
		"max_replication_slots":        "10",
		"max_slot_wal_keep_size":       "1GB",
		"wal_keep_size":                "128MB",
		"synchronous_commit":           "on",
		"recovery_target_timeline":     "latest",
		"wal_sender_timeout":           durationMS(WalSenderTimeout),
		"wal_receiver_timeout":         durationMS(WalReceiverTimeout),
		"wal_receiver_status_interval": "1s",
		"wal_retrieve_retry_interval":  "1s",
		"password_encryption":          "scram-sha-256",
		"logging_collector":            "off",
		"log_replication_commands":     "on",
	}
}

// StaticFlags renders StaticParameters as sorted postgres arguments. No
// value may contain whitespace: the instance script word-splits them.
func StaticFlags() []string {
	p := StaticParameters()
	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	out := make([]string, 0, 2*len(keys))
	for _, k := range keys {
		out = append(out, "-c", k+"="+p[k])
	}
	return out
}

// SyncStandbyNames renders synchronous_standby_names for a primary whose
// potential synchronous standbys are the given application names. It
// returns "" (asynchronous) when n is zero or there are no standbys.
func SyncStandbyNames(method string, n int, standbys []string) string {
	if n <= 0 || len(standbys) == 0 {
		return ""
	}
	names := slices.Clone(standbys)
	slices.Sort(names)
	quoted := make([]string, len(names))
	for i, s := range names {
		quoted[i] = QuoteIdent(s)
	}
	return fmt.Sprintf("%s %d (%s)", strings.ToUpper(method), n, strings.Join(quoted, ", "))
}

// PrimaryConninfo is the libpq connection string a standby uses to stream
// from host. The password is read from the passfile the instance scripts
// write, so it never lands in postgresql.auto.conf.
func PrimaryConninfo(host, applicationName string) string {
	return strings.Join([]string{
		"host=" + host,
		fmt.Sprintf("port=%d", Port),
		"user=" + ReplicationUser,
		"application_name=" + applicationName,
		"passfile=/var/lib/postgresql/data/.pgpass",
		"sslmode=prefer",
		"connect_timeout=2",
		// Detect a silent peer at the TCP level well before
		// wal_receiver_timeout on the next reconnect attempt.
		"keepalives=1",
		"keepalives_idle=2",
		"keepalives_interval=1",
		"keepalives_count=3",
		"tcp_user_timeout=5000",
	}, " ")
}

// SlotName maps an instance name to its physical replication slot name.
// Slot names only allow lower-case letters, digits and underscores.
func SlotName(instance string) string {
	var b strings.Builder
	b.WriteString(SlotPrefix)
	for _, r := range strings.ToLower(instance) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// QuoteIdent double-quotes an identifier.
func QuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// QuoteLiteral single-quotes a string literal. ALTER SYSTEM does not accept
// bind parameters, so values are inlined.
func QuoteLiteral(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}

// HBA is the pg_hba.conf every instance uses. Local socket access is trust
// (only processes inside the pod reach /tmp), everything over the network
// needs SCRAM.
const HBA = `# managed by pg-operator
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    replication     replicator      0.0.0.0/0               scram-sha-256
host    replication     replicator      ::/0                    scram-sha-256
host    all             all             0.0.0.0/0               scram-sha-256
host    all             all             ::/0                    scram-sha-256
`

func durationMS(d time.Duration) string {
	return fmt.Sprintf("%dms", d.Milliseconds())
}
