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
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Target identifies an instance to talk to.
type Target struct {
	// Name is the instance (pod) name.
	Name string
	// Host is the pod IP. Pod IPs are used instead of DNS so that a probe
	// never resolves to a different pod than the one being judged.
	Host string
	// Password of the superuser.
	Password string
}

// ReplicaConn is one row of pg_stat_replication on a primary.
type ReplicaConn struct {
	ApplicationName string
	State           string
	SyncState       string
	FlushLSN        LSN
	// ReplyAge is the time since the standby last reported its position;
	// negative when unknown.
	ReplyAge time.Duration
}

// Observation is a point-in-time view of one instance.
type Observation struct {
	Name      string
	Reachable bool
	Err       error

	InRecovery bool
	Timeline   int64
	// ReceiveLSN is pg_last_wal_receive_lsn() on a standby and
	// pg_current_wal_flush_lsn() on a primary.
	ReceiveLSN LSN
	ReplayLSN  LSN

	// Standby only: WAL receiver state.
	Streaming    bool
	UpstreamHost string
	LastMsgAge   time.Duration

	// Primary only: connected standbys.
	Replicas []ReplicaConn

	// Settings holds the current value of the dynamic settings the
	// operator converges.
	Settings map[string]string
}

// StreamingReplicas returns the application names of standbys that are
// streaming and have replied recently.
func (o Observation) StreamingReplicas(maxReplyAge time.Duration) []string {
	var out []string
	for _, r := range o.Replicas {
		if r.State == "streaming" && (r.ReplyAge < 0 || r.ReplyAge <= maxReplyAge) {
			out = append(out, r.ApplicationName)
		}
	}
	return out
}

// Client is the set of SQL operations the controller performs. It is an
// interface so the state machine can be tested against a fake.
type Client interface {
	Observe(ctx context.Context, t Target) Observation
	Promote(ctx context.Context, t Target, wait time.Duration) error
	Fence(ctx context.Context, t Target) error
	ApplySettings(ctx context.Context, t Target, settings map[string]string) ([]string, error)
	EnsureSlots(ctx context.Context, t Target, want []string) error
	Checkpoint(ctx context.Context, t Target) error
}

// DynamicSettings are read back by Observe and written by ApplySettings.
var DynamicSettings = []string{
	"synchronous_standby_names",
	"primary_conninfo",
	"primary_slot_name",
	"default_transaction_read_only",
}

// SQLClient implements Client with a fresh connection per call. A new
// connection per probe is intentional: it exercises the same path a client
// takes (TCP accept, auth, backend fork), so a postmaster that is up but
// unable to serve new sessions is reported as failed.
type SQLClient struct{}

var _ Client = SQLClient{}

func (SQLClient) connect(ctx context.Context, t Target) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%d user=%s dbname=postgres sslmode=prefer application_name=pg-operator",
		t.Host, Port, SuperUser))
	if err != nil {
		return nil, err
	}
	cfg.Password = t.Password
	if dl, ok := ctx.Deadline(); ok {
		cfg.ConnectTimeout = time.Until(dl)
	}
	// Simple protocol avoids a prepare round trip per statement and
	// works for utility statements such as ALTER SYSTEM and CHECKPOINT.
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	return pgx.ConnectConfig(ctx, cfg)
}

const observeSQL = `
SELECT pg_is_in_recovery(),
       coalesce(pg_last_wal_receive_lsn()::text, ''),
       coalesce(pg_last_wal_replay_lsn()::text, ''),
       CASE WHEN pg_is_in_recovery() THEN '' ELSE pg_current_wal_flush_lsn()::text END,
       CASE WHEN pg_is_in_recovery()
            THEN coalesce((SELECT received_tli::bigint FROM pg_stat_wal_receiver),
                          (SELECT timeline_id::bigint FROM pg_control_checkpoint()))
            ELSE ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::bigint
       END`

const settingsSQL = `SELECT name, setting FROM pg_settings WHERE name = ANY($1)`

const walReceiverSQL = `
SELECT status, coalesce(sender_host, ''),
       coalesce(extract(epoch FROM now() - last_msg_receipt_time), -1)::float8
  FROM pg_stat_wal_receiver`

const replicationSQL = `
SELECT application_name, state, sync_state, coalesce(flush_lsn::text, ''),
       coalesce(extract(epoch FROM now() - reply_time), -1)::float8
  FROM pg_stat_replication`

// Observe never returns an error: an unreachable instance is a valid
// observation with Reachable=false and Err set.
func (c SQLClient) Observe(ctx context.Context, t Target) Observation {
	o := Observation{Name: t.Name}
	if t.Host == "" {
		o.Err = errors.New("instance has no pod IP")
		return o
	}
	conn, err := c.connect(ctx, t)
	if err != nil {
		o.Err = err
		return o
	}
	defer closeConn(ctx, conn)

	var recv, replay, flush string
	if err := conn.QueryRow(ctx, observeSQL).Scan(&o.InRecovery, &recv, &replay, &flush, &o.Timeline); err != nil {
		o.Err = fmt.Errorf("observe: %w", err)
		return o
	}
	if o.InRecovery {
		o.ReceiveLSN, _ = ParseLSN(recv)
		o.ReplayLSN, _ = ParseLSN(replay)
	} else {
		o.ReceiveLSN, _ = ParseLSN(flush)
		o.ReplayLSN = o.ReceiveLSN
	}

	o.Settings = map[string]string{}
	rows, err := conn.Query(ctx, settingsSQL, DynamicSettings)
	if err != nil {
		o.Err = fmt.Errorf("settings: %w", err)
		return o
	}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			o.Err = err
			return o
		}
		o.Settings[k] = v
	}
	if err := rows.Err(); err != nil {
		o.Err = err
		return o
	}

	if o.InRecovery {
		var status string
		var age float64
		err := conn.QueryRow(ctx, walReceiverSQL).Scan(&status, &o.UpstreamHost, &age)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// No WAL receiver process at all: not streaming.
		case err != nil:
			o.Err = fmt.Errorf("wal receiver: %w", err)
			return o
		default:
			o.Streaming = status == "streaming"
			o.LastMsgAge = seconds(age)
		}
	} else {
		rows, err := conn.Query(ctx, replicationSQL)
		if err != nil {
			o.Err = fmt.Errorf("replication: %w", err)
			return o
		}
		for rows.Next() {
			var r ReplicaConn
			var lsn string
			var age float64
			if err := rows.Scan(&r.ApplicationName, &r.State, &r.SyncState, &lsn, &age); err != nil {
				o.Err = err
				return o
			}
			r.FlushLSN, _ = ParseLSN(lsn)
			r.ReplyAge = seconds(age)
			o.Replicas = append(o.Replicas, r)
		}
		if err := rows.Err(); err != nil {
			o.Err = err
			return o
		}
	}
	o.Reachable = true
	return o
}

// Promote runs pg_promote and waits up to wait for the instance to leave
// recovery. Promoting an instance that is already a primary is a no-op,
// which is what makes a retried Promoting phase safe.
func (c SQLClient) Promote(ctx context.Context, t Target, wait time.Duration) error {
	conn, err := c.connect(ctx, t)
	if err != nil {
		return err
	}
	defer closeConn(ctx, conn)

	var inRecovery bool
	if err := conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return err
	}
	if !inRecovery {
		return nil
	}
	var ok bool
	if err := conn.QueryRow(ctx, "SELECT pg_promote(true, $1)", int(wait.Seconds())).Scan(&ok); err != nil {
		return fmt.Errorf("pg_promote: %w", err)
	}
	if !ok {
		return fmt.Errorf("pg_promote did not complete within %s", wait)
	}
	return nil
}

// Fence makes a reachable primary refuse writes from new transactions and
// disconnects every client session. It is best effort: when the old
// primary cannot be reached, synchronous replication is what stops it
// from acknowledging writes (see docs/design.md).
func (c SQLClient) Fence(ctx context.Context, t Target) error {
	conn, err := c.connect(ctx, t)
	if err != nil {
		return err
	}
	defer closeConn(ctx, conn)

	for _, q := range []string{
		"ALTER SYSTEM SET default_transaction_read_only = on",
		"SELECT pg_reload_conf()",
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		  WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("fence: %w", err)
		}
	}
	return nil
}

// ApplySettings converges the given settings with ALTER SYSTEM and reloads
// the configuration if anything changed. It returns the names of the
// settings that were changed. ALTER SYSTEM works on standbys too, which
// lets the operator pre-stage synchronous_standby_names on every standby
// so a promoted instance enforces the quorum from its first commit.
func (c SQLClient) ApplySettings(ctx context.Context, t Target, settings map[string]string) ([]string, error) {
	conn, err := c.connect(ctx, t)
	if err != nil {
		return nil, err
	}
	defer closeConn(ctx, conn)

	names := make([]string, 0, len(settings))
	for k := range settings {
		names = append(names, k)
	}
	slices.Sort(names)

	current := map[string]string{}
	rows, err := conn.Query(ctx, settingsSQL, names)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		current[k] = v
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var changed []string
	for _, k := range names {
		if current[k] == settings[k] {
			continue
		}
		q := fmt.Sprintf("ALTER SYSTEM SET %s = %s", k, QuoteLiteral(settings[k]))
		if settings[k] == "" {
			q = fmt.Sprintf("ALTER SYSTEM RESET %s", k)
		}
		if _, err := conn.Exec(ctx, q); err != nil {
			return changed, fmt.Errorf("%s: %w", k, err)
		}
		changed = append(changed, k)
	}
	if len(changed) > 0 {
		if _, err := conn.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
			return changed, err
		}
	}
	return changed, nil
}

// EnsureSlots makes the operator-owned physical slots on t exactly the
// given set. Slots are created with immediately_reserve so WAL needed by a
// standby that is still being cloned is retained. Only inactive slots are
// dropped, and only those with SlotPrefix.
func (c SQLClient) EnsureSlots(ctx context.Context, t Target, want []string) error {
	conn, err := c.connect(ctx, t)
	if err != nil {
		return err
	}
	defer closeConn(ctx, conn)

	rows, err := conn.Query(ctx, `SELECT slot_name, active FROM pg_replication_slots
	                               WHERE slot_type = 'physical' AND slot_name LIKE $1`, SlotPrefix+"%")
	if err != nil {
		return err
	}
	have := map[string]bool{}
	for rows.Next() {
		var name string
		var active bool
		if err := rows.Scan(&name, &active); err != nil {
			return err
		}
		have[name] = active
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, s := range want {
		if _, ok := have[s]; ok {
			continue
		}
		if _, err := conn.Exec(ctx, "SELECT pg_create_physical_replication_slot($1, true)", s); err != nil &&
			!strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("create slot %s: %w", s, err)
		}
	}
	for s, active := range have {
		if active || slices.Contains(want, s) {
			continue
		}
		if _, err := conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", s); err != nil {
			return fmt.Errorf("drop slot %s: %w", s, err)
		}
	}
	return nil
}

// Checkpoint forces a checkpoint, shortening crash recovery and the WAL a
// rewinding peer has to fetch before a planned switchover.
func (c SQLClient) Checkpoint(ctx context.Context, t Target) error {
	conn, err := c.connect(ctx, t)
	if err != nil {
		return err
	}
	defer closeConn(ctx, conn)
	_, err = conn.Exec(ctx, "CHECKPOINT")
	return err
}

func closeConn(ctx context.Context, conn *pgx.Conn) {
	_ = conn.Close(context.WithoutCancel(ctx))
}

func seconds(f float64) time.Duration {
	if f < 0 {
		return -1
	}
	return time.Duration(f * float64(time.Second))
}
