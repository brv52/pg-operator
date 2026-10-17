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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Config of one workload run.
type Config struct {
	// DSN of the read-write endpoint, e.g. host=db-rw user=postgres ...
	DSN string
	// Workers is the number of concurrent clients.
	Workers int
	// Keys is the number of registers; KeyBase offsets them so that every
	// run starts from fresh registers (all zero) without truncating.
	Keys    int
	KeyBase int64
	// Duration of the run.
	Duration time.Duration
	// OpTimeout bounds one operation, including connecting.
	OpTimeout time.Duration
	// Think is the pause between two operations of one client.
	Think time.Duration
	// JournalPath receives every acknowledged write, fsynced before the
	// client issues its next operation.
	JournalPath string
	// Seed makes the operation mix reproducible.
	Seed uint64
}

const schema = `
CREATE TABLE IF NOT EXISTS register (
    key     bigint PRIMARY KEY,
    value   bigint NOT NULL,
    version bigint NOT NULL,
    tx_id   text   NOT NULL
);
CREATE TABLE IF NOT EXISTS write_log (
    tx_id text PRIMARY KEY,
    key   bigint NOT NULL,
    value bigint NOT NULL,
    at    timestamptz NOT NULL DEFAULT now()
)`

// connect opens a connection that reports whether the server warned that
// a commit's wait for synchronous replication was cancelled.
func connect(ctx context.Context, dsn string, syncCancelled *bool) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		// "canceling wait for synchronous replication ... the
		// transaction has already committed locally, but might not have
		// been replicated to the standby."
		if strings.Contains(n.Message, "synchronous replication") || strings.Contains(n.Detail, "committed locally") {
			*syncCancelled = true
		}
	}
	return pgx.ConnectConfig(ctx, cfg)
}

// Setup creates the tables and the run's registers (all zero), retrying
// until the endpoint accepts writes or ctx expires.
func Setup(ctx context.Context, cfg Config) error {
	for {
		err := setupOnce(ctx, cfg)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("setup: %w (last error: %v)", ctx.Err(), err)
		case <-time.After(time.Second):
		}
	}
}

func setupOnce(ctx context.Context, cfg Config) error {
	var flag bool
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := connect(cctx, cfg.DSN, &flag)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	if _, err := conn.Exec(cctx, schema); err != nil {
		return err
	}
	_, err = conn.Exec(cctx, `INSERT INTO register (key, value, version, tx_id)
	    SELECT k, 0, 0, 'init' FROM generate_series($1::bigint, $2::bigint) k ON CONFLICT DO NOTHING`,
		cfg.KeyBase, cfg.KeyBase+int64(cfg.Keys)-1)
	return err
}

type journal struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

func openJournal(path string) (*journal, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &journal{f: f, w: bufio.NewWriter(f)}, nil
}

func (j *journal) record(op Op) error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	b, _ := json.Marshal(op)
	if _, err := j.w.Write(append(b, '\n')); err != nil {
		return err
	}
	if err := j.w.Flush(); err != nil {
		return err
	}
	return j.f.Sync()
}

func (j *journal) close() {
	if j != nil {
		_ = j.f.Close()
	}
}

// Run drives the workload for cfg.Duration and returns the history.
func Run(ctx context.Context, cfg Config) ([]Op, error) {
	j, err := openJournal(cfg.JournalPath)
	if err != nil {
		return nil, err
	}
	defer j.close()

	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	histories := make([][]Op, cfg.Workers)
	var wg sync.WaitGroup
	for w := range cfg.Workers {
		wg.Go(func() {
			c := &client{id: w, cfg: cfg, journal: j,
				rng: rand.New(rand.NewPCG(cfg.Seed, uint64(w)+1))}
			histories[w] = c.loop(ctx)
		})
	}
	wg.Wait()

	var all []Op
	for _, h := range histories {
		all = append(all, h...)
	}
	return all, nil
}

type client struct {
	id      int
	cfg     Config
	journal *journal
	rng     *rand.Rand

	conn          *pgx.Conn
	syncCancelled bool
	seq           int64
	lastSeen      map[int64]int64
}

func (c *client) loop(ctx context.Context) []Op {
	c.lastSeen = map[int64]int64{}
	var history []Op
	for ctx.Err() == nil {
		op := c.next()
		c.do(ctx, &op)
		history = append(history, op)
		if op.Status == StatusOK && writes(op) {
			_ = c.journal.record(op)
		}
		if op.Status != StatusOK && c.conn != nil {
			// Never reuse a connection after an error: it may be pinned
			// to an old primary that the Service no longer selects.
			_ = c.conn.Close(context.Background())
			c.conn = nil
		}
		select {
		case <-ctx.Done():
		case <-time.After(c.cfg.Think):
		}
	}
	if c.conn != nil {
		_ = c.conn.Close(context.Background())
	}
	return history
}

// next picks the next operation: 40% reads, 30% blind writes, 30% CAS
// from the value this client last saw. Written values are unique across
// the run, which keeps the linearizability search narrow.
func (c *client) next() Op {
	key := c.cfg.KeyBase + int64(c.rng.IntN(c.cfg.Keys))
	c.seq++
	unique := int64(c.id+1)*1_000_000_000 + c.seq
	op := Op{Client: c.id, Key: key}
	switch r := c.rng.IntN(10); {
	case r < 4:
		op.Kind = KindRead
	case r < 7:
		op.Kind = KindWrite
		op.Value = unique
		op.TxID = fmt.Sprintf("%d-%d", c.id, c.seq)
	default:
		op.Kind = KindCAS
		op.From = c.lastSeen[key]
		op.Value = unique
		op.TxID = fmt.Sprintf("%d-%d", c.id, c.seq)
	}
	return op
}

func (c *client) do(parent context.Context, op *Op) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), c.cfg.OpTimeout)
	defer cancel()

	op.Call = time.Now().UnixNano()
	defer func() { op.Return = time.Now().UnixNano() }()

	if c.conn == nil {
		conn, err := connect(ctx, c.cfg.DSN, &c.syncCancelled)
		if err != nil {
			// Nothing was sent: the operation definitely did not happen.
			op.Status, op.Error = StatusFail, err.Error()
			return
		}
		c.conn = conn
	}

	switch op.Kind {
	case KindRead:
		var v int64
		err := c.conn.QueryRow(ctx, "SELECT value FROM register WHERE key = $1", op.Key).Scan(&v)
		if err != nil {
			op.Status, op.Error = StatusFail, err.Error()
			return
		}
		op.Value, op.Status = v, StatusOK
		c.lastSeen[op.Key] = v
	case KindWrite, KindCAS:
		c.write(ctx, op)
	}
}

// write runs a write or CAS in an explicit transaction that also records
// the transaction ID in write_log, so the final reconciliation can tell
// exactly which writes survived.
func (c *client) write(ctx context.Context, op *Op) {
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		op.Status, op.Error = StatusFail, err.Error()
		return
	}
	// Any error before COMMIT is sent aborts the transaction: if the
	// connection broke, the server rolls back the open transaction.
	abort := func(err error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		op.Status, op.Error = StatusFail, err.Error()
	}

	if op.Kind == KindWrite {
		_, err = tx.Exec(ctx, `UPDATE register SET value = $2, version = version + 1, tx_id = $3 WHERE key = $1`,
			op.Key, op.Value, op.TxID)
		if err != nil {
			abort(err)
			return
		}
	} else {
		tag, err := tx.Exec(ctx, `UPDATE register SET value = $3, version = version + 1, tx_id = $4
		                           WHERE key = $1 AND value = $2`, op.Key, op.From, op.Value, op.TxID)
		if err != nil {
			abort(err)
			return
		}
		if tag.RowsAffected() == 0 {
			// The comparison failed: nothing changed, and the outcome is
			// known as soon as the statement returned.
			if err := tx.Rollback(ctx); err != nil {
				op.Status, op.Error = StatusInfo, err.Error()
				return
			}
			op.Status, op.OK = StatusOK, false
			op.TxID = ""
			return
		}
		op.OK = true
	}
	if _, err := tx.Exec(ctx, `INSERT INTO write_log (tx_id, key, value) VALUES ($1, $2, $3)`,
		op.TxID, op.Key, op.Value); err != nil {
		abort(err)
		return
	}

	c.syncCancelled = false
	if err := tx.Commit(ctx); err != nil {
		// The COMMIT may or may not have been executed.
		op.Status, op.Error = StatusInfo, err.Error()
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "25006" {
			// read_only_sql_transaction: rejected by a fenced primary.
			op.Status = StatusFail
		}
		return
	}
	if c.syncCancelled {
		// Committed locally only; a failover can legitimately lose it.
		op.Status, op.Error = StatusInfo, "commit returned after cancelling the synchronous replication wait"
		return
	}
	op.Status = StatusOK
	c.lastSeen[op.Key] = op.Value
}

// FinalState waits for a writable primary and reads back the run's
// registers (as one final read per key, appended to the history) and the
// set of transaction IDs in write_log.
func FinalState(ctx context.Context, cfg Config, client int) ([]Op, map[string]bool, error) {
	var lastErr error
	for {
		reads, found, err := finalStateOnce(ctx, cfg, client)
		if err == nil {
			return reads, found, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("final state: %w (last error: %v)", ctx.Err(), lastErr)
		case <-time.After(time.Second):
		}
	}
}

func finalStateOnce(ctx context.Context, cfg Config, client int) ([]Op, map[string]bool, error) {
	var flag bool
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := connect(cctx, cfg.DSN, &flag)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	var inRecovery bool
	if err := conn.QueryRow(cctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return nil, nil, err
	}
	if inRecovery {
		return nil, nil, errors.New("endpoint is a standby")
	}

	var reads []Op
	for k := cfg.KeyBase; k < cfg.KeyBase+int64(cfg.Keys); k++ {
		op := Op{Client: client, Kind: KindRead, Key: k, Call: time.Now().UnixNano()}
		if err := conn.QueryRow(cctx, "SELECT value FROM register WHERE key = $1", k).Scan(&op.Value); err != nil {
			return nil, nil, err
		}
		op.Return, op.Status = time.Now().UnixNano(), StatusOK
		reads = append(reads, op)
	}

	rows, err := conn.Query(cctx, "SELECT tx_id FROM write_log WHERE key BETWEEN $1 AND $2",
		cfg.KeyBase, cfg.KeyBase+int64(cfg.Keys)-1)
	if err != nil {
		return nil, nil, err
	}
	found := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, nil, err
		}
		found[id] = true
	}
	return reads, found, rows.Err()
}
