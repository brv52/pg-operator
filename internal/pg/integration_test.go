//go:build integration

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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The integration test runs the real instance script and SQL client against
// PostgreSQL containers on a private Docker network:
//
//	bootstrap primary -> clone two standbys -> diverge the primary while it
//	is cut off -> kill it -> elect and promote -> repoint -> rejoin the old
//	primary through pg_rewind -> check the diverged write is gone
//
// Run with: make test-integration

const (
	image    = "postgres:18.6-bookworm"
	superPW  = "super-secret"
	replPW   = "repl-secret"
	pgdata   = "/var/lib/postgresql/data/pgdata"
	waitTime = 90 * time.Second
)

type harness struct {
	t       *testing.T
	prefix  string
	network string
	scripts string
}

func newHarness(t *testing.T) *harness {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "instance.sh"), []byte(InstanceScript), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pg_hba.conf"), []byte(HBA), 0o644); err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, prefix: fmt.Sprintf("pgit%d", time.Now().UnixNano()%100000), scripts: dir}
	h.network = h.prefix + "-net"
	h.docker("network", "create", h.network)
	t.Cleanup(func() {
		out, _ := exec.Command("docker", "ps", "-aq", "--filter", "name="+h.prefix).Output()
		for _, id := range strings.Fields(string(out)) {
			_ = exec.Command("docker", "rm", "-f", "-v", id).Run()
		}
		out, _ = exec.Command("docker", "volume", "ls", "-q", "--filter", "name="+h.prefix).Output()
		for _, v := range strings.Fields(string(out)) {
			_ = exec.Command("docker", "volume", "rm", "-f", v).Run()
		}
		_ = exec.Command("docker", "network", "rm", h.network).Run()
	})
	return h
}

func (h *harness) docker(args ...string) string {
	h.t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		h.t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// start runs an instance container. name doubles as the hostname, the
// application_name and the volume name, like a pod and its PVC.
func (h *harness) start(name, role, primaryHost string) Target {
	h.t.Helper()
	flags := strings.Join(StaticFlags(), " ")
	c := h.prefix + "-" + name
	h.docker("run", "-d", "--name", c, "--hostname", name, "--network", h.network, "--network-alias", name,
		"--user", "999:999",
		"-v", h.prefix+"-"+name+":/var/lib/postgresql",
		"-v", h.scripts+"/instance.sh:/scripts/instance.sh:ro",
		"-v", h.scripts+"/pg_hba.conf:/etc/pg/pg_hba.conf:ro",
		"-e", "POD_NAME="+name, "-e", "ROLE="+role, "-e", "PRIMARY_HOST="+primaryHost,
		"-e", "PGDATA="+pgdata, "-e", "SUPERUSER_PASSWORD="+superPW, "-e", "REPLICATION_PASSWORD="+replPW,
		"-e", "SLOT_NAME="+SlotName(name), "-e", "STATIC_FLAGS="+flags,
		"--entrypoint", "sh", image, "-c", "/scripts/instance.sh init && exec /scripts/instance.sh run")
	ip := h.docker("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", c)
	t := Target{Name: name, Host: ip, Password: superPW}
	h.waitReady(t)
	return t
}

func (h *harness) waitReady(t Target) {
	h.t.Helper()
	deadline := time.Now().Add(waitTime)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		o := SQLClient{}.Observe(ctx, t)
		cancel()
		if o.Reachable {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	logs, _ := exec.Command("docker", "logs", h.prefix+"-"+t.Name).CombinedOutput()
	h.t.Fatalf("%s not ready after %s\n%s", t.Name, waitTime, logs)
}

func (h *harness) exec(t Target, sql string, args ...any) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("host=%s user=postgres password=%s dbname=postgres", t.Host, superPW))
	if err != nil {
		h.t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, sql, args...); err != nil {
		h.t.Fatalf("%s on %s: %v", sql, t.Name, err)
	}
}

func (h *harness) count(t Target, where string) int {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("host=%s user=postgres password=%s dbname=postgres", t.Host, superPW))
	if err != nil {
		h.t.Fatal(err)
	}
	defer conn.Close(ctx)
	var n int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM kv WHERE "+where).Scan(&n); err != nil {
		h.t.Fatal(err)
	}
	return n
}

func (h *harness) eventually(what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(waitTime)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s", what)
}

func observe(t Target) Observation {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return SQLClient{}.Observe(ctx, t)
}

func TestFailoverAndRewind(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	c := SQLClient{}

	p1 := h.start("pg1", "primary", "")
	if err := c.EnsureSlots(ctx, p1, []string{SlotName("pg2"), SlotName("pg3")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplySettings(ctx, p1, map[string]string{
		"synchronous_standby_names": SyncStandbyNames("any", 1, []string{"pg2", "pg3"}),
	}); err != nil {
		t.Fatal(err)
	}

	p2 := h.start("pg2", "replica", "pg1")
	p3 := h.start("pg3", "replica", "pg1")

	h.eventually("both standbys streaming", func() bool {
		return len(observe(p1).StreamingReplicas(10*time.Second)) == 2
	})

	h.exec(p1, "CREATE TABLE kv (k int PRIMARY KEY, src text)")
	for i := 0; i < 100; i++ {
		h.exec(p1, "INSERT INTO kv VALUES ($1, 'acked')", i)
	}

	// Standbys can be re-pointed with ALTER SYSTEM while in recovery, which
	// the Reconfiguring phase relies on.
	if _, err := c.ApplySettings(ctx, p2, map[string]string{
		"synchronous_standby_names": SyncStandbyNames("any", 1, []string{"pg1", "pg3"}),
	}); err != nil {
		t.Fatalf("ALTER SYSTEM on a standby: %v", err)
	}

	primaryTimeline := observe(p1).Timeline

	// Cut the primary off and make it commit something no standby has.
	// synchronous_commit=local is a per-session downgrade any client can
	// make; it is exactly the write that pg_rewind must throw away.
	h.docker("network", "disconnect", h.network, h.prefix+"-pg1")
	h.docker("exec", h.prefix+"-pg1", "psql", "-h", "/tmp", "-U", "postgres", "-c",
		"SET synchronous_commit = local; INSERT INTO kv SELECT g, 'diverged' FROM generate_series(1000, 1100) g")
	h.docker("kill", h.prefix+"-pg1")

	var cands []Candidate
	for i, s := range []Target{p2, p3} {
		h.eventually(s.Name+" reachable", func() bool { return observe(s).Reachable })
		// The primary is gone, so the WAL receiver has exited and
		// pg_stat_wal_receiver is empty: the timeline must still come out
		// right, or the election would rank by a stale value.
		h.eventually(s.Name+" receiver gone", func() bool { o := observe(s); return !o.Streaming })
		o := observe(s)
		if o.Timeline != primaryTimeline {
			t.Fatalf("%s reports timeline %d with its WAL receiver down, primary was on %d", s.Name, o.Timeline, primaryTimeline)
		}
		cands = append(cands, Candidate{Name: s.Name, Ordinal: i + 2, Reachable: o.Reachable,
			InRecovery: o.InRecovery, Timeline: o.Timeline, ReceiveLSN: o.ReceiveLSN, ReplayLSN: o.ReplayLSN})
	}
	winner, err := Elect(cands, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	newPrimary, follower := p2, p3
	if winner.Name == "pg3" {
		newPrimary, follower = p3, p2
	}

	if err := c.EnsureSlots(ctx, newPrimary, []string{SlotName("pg1"), SlotName(follower.Name)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Promote(ctx, newPrimary, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.Promote(ctx, newPrimary, 30*time.Second); err != nil {
		t.Fatalf("second promote must be a no-op: %v", err)
	}
	if err := c.Checkpoint(ctx, newPrimary); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplySettings(ctx, newPrimary, map[string]string{
		"synchronous_standby_names": SyncStandbyNames("any", 1, []string{"pg1", follower.Name}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplySettings(ctx, follower, map[string]string{
		"primary_conninfo": PrimaryConninfo(newPrimary.Name, follower.Name),
	}); err != nil {
		t.Fatal(err)
	}
	h.eventually("follower streaming from new primary", func() bool {
		return len(observe(newPrimary).StreamingReplicas(10*time.Second)) == 1
	})

	if got := h.count(newPrimary, "src = 'acked'"); got != 100 {
		t.Fatalf("new primary has %d acknowledged rows, want 100", got)
	}
	h.exec(newPrimary, "INSERT INTO kv VALUES (200, 'after-failover')")

	// Bring the old primary back the way the operator does: a new pod on
	// the same volume, created as a replica of the new primary.
	h.docker("rm", "-f", h.prefix+"-pg1")
	p1 = h.start("pg1", "replica", newPrimary.Name)
	h.eventually("old primary streaming after rewind", func() bool {
		o := observe(p1)
		return o.InRecovery && o.Streaming
	})
	h.eventually("old primary caught up", func() bool { return h.count(p1, "k = 200") == 1 })
	if got := h.count(p1, "src = 'diverged'"); got != 0 {
		t.Fatalf("rewound instance still has %d diverged rows", got)
	}
	if o := observe(p1); o.Settings["default_transaction_read_only"] != "off" {
		t.Fatalf("rejoined instance kept read_only=%s", o.Settings["default_transaction_read_only"])
	}
	// Second failure, now on timeline 2: the two remaining standbys (one
	// re-pointed, one rewound) must both report timeline 2 once their WAL
	// receivers are down, without a checkpoint having moved their
	// restartpoint onto the new timeline.
	h.docker("kill", h.prefix+"-"+newPrimary.Name)
	for _, s := range []Target{p1, follower} {
		h.eventually(s.Name+" receiver gone", func() bool { o := observe(s); return o.Reachable && !o.Streaming })
		if o := observe(s); o.Timeline != primaryTimeline+1 {
			t.Fatalf("%s reports timeline %d after the second failure, want %d", s.Name, o.Timeline, primaryTimeline+1)
		}
	}

	logs := h.docker("logs", h.prefix+"-pg1")
	if !strings.Contains(logs, "rewinding against") || strings.Contains(logs, "falling back to a full clone") {
		t.Fatalf("expected a pg_rewind rejoin without clone fallback:\n%s", logs)
	}
}
