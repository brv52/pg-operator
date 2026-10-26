# Design

pg-operator runs a PostgreSQL cluster of one primary and N-1 streaming
standbys and fails over without losing a write that a client saw
committed. This document explains how, and which choices that forces.
[failure-modes.md](failure-modes.md) lists what happens under each fault,
including the bugs the chaos runs found.

## Goals and non-goals

- **No acknowledged write is ever lost, and no client ever sees two
  primaries.** Availability is traded for this whenever the two conflict:
  a cluster that cannot prove a standby has every acknowledged commit
  stops accepting writes instead of promoting.
- **Fail over in seconds, not in node-failure-detection time.** Kubernetes
  marks a node `NotReady` after 50 s; the operator watches PostgreSQL
  itself and does not wait for that.
- **Measured, not claimed.** Every failover path is exercised by a
  Porcupine linearizability checker under fault injection, and the same
  harness runs against CloudNativePG.
- Not covered: backups and PITR, major version upgrades, multi-cluster
  replicas, connection pooling. The operator is a study of the failover
  core, not a replacement for a full-featured operator.

## Resources

One `PostgresCluster` (`pgc`) object owns:

| Object | Purpose |
|---|---|
| Pods `NAME-1..N` | Instances, created directly (no StatefulSet) so the operator decides which pod to replace and in which role. |
| PVCs `NAME-1..N` | One volume per instance; they outlive the pods. |
| Service `NAME-rw` | Selects `role=primary`. Clients write here. |
| Service `NAME-ro` | Selects `role=replica`. |
| Service `NAME-instances` | Headless; stable DNS for replication. |
| Lease `NAME-primary` | Cluster-visible record of the primary and its epoch. |
| PodDisruptionBudgets | One for the primary (holds a drain until the switchover), one for the standbys (at most one voluntary eviction). |
| Secret `NAME-credentials` | Superuser and replication passwords. |

StatefulSets were ruled out because their pods are interchangeable by
design: the controller recreates `NAME-0` with the same spec whatever
it was before. Here the role of a recreated pod depends on what happened:
a former primary may only come back as a standby, and a standby that
cannot follow the new timeline needs `pg_rewind` or a fresh clone.

Routing is done only with labels. Fencing the old primary means
relabelling it `role=fenced`, which matches neither Service, so the
`-rw` endpoint goes empty before anything else happens.

## Replication and the quorum

Replication is physical streaming with one slot per standby and
`synchronous_standby_names = ANY n (s1, ..., sm)`. A commit is acknowledged
only after `n` of the `m` standbys have flushed it. `n` defaults to 1 and
the admission webhook (CEL) requires `n < instances`.

The election rule follows from that. Any `m-n+1` standbys intersect every
set of `n` standbys, so the most advanced of `m-n+1` reachable standbys
holds every acknowledged commit (`pg.RequiredVoters`). With three
instances and `ANY 1`, both standbys must answer. If one cannot, the
operator does not promote the one it can see, because the unseen one may
be the only holder of the last commit.

`synchronous_standby_names` is also set on standbys, where PostgreSQL
ignores it, so that a promoted standby enforces the quorum from its first
commit with no window of asynchronous acknowledgements.

## The failover state machine

The reconciler probes every instance each `probeInterval` (1 s by
default) and runs one round of:

```
Healthy ──K failed rounds──▶ Fencing ──▶ Promoting ──▶ Reconfiguring ──▶ Healthy
   │  ▲
   └──┴── Detecting (0 < failures < K)
```

Every transition is written to `.status` before the next phase's side
effects run, and every phase is idempotent. An operator that crashes at
any point restarts the phase it was in.

**Detecting.** A round fails if the primary's pod is missing or
terminating, its SQL probe fails, it reports `pg_is_in_recovery()`, or it
is isolated from its standbys (below). Pod deletion is definitive and
skips the K-round wait; a failed probe needs K consecutive failures
(`failureThresholdK`, default 3). Failover is not started at all when no
standby could be elected. That would only turn a slow primary into an
outage that needs a human. The operator recreates the primary's pod in
place instead and keeps watching.

**Fencing.** The old primary is relabelled `fenced`. If it is reachable
it is also made read-only with its sessions dropped. The epoch is bumped
in `.status` and in the Lease with no holder, and the pod is deleted. A
failed primary is deleted with grace 0 on a Ready node. A switchover uses
the normal grace period so the fast shutdown ships the final WAL. On an
unreachable node it waits until the pod is really gone, since a grace-0
delete there removes only the API object.

**Promoting.** Every reachable standby is *frozen* first: its
`primary_conninfo` is cleared so its WAL receiver stops. Without that, a
dying primary's last records (the shutdown checkpoint) can reach one
standby after the LSNs were sampled. The loser then ends up one record
past the fork point of the winner's new timeline and can never follow it.
The election picks the highest timeline, then received LSN, then replayed
LSN, then lowest ordinal. The winner's name is persisted *before*
`pg_promote()`, because after promotion the LSNs change and a restarted
operator could elect someone else. If the winner stays unreachable it is
abandoned and its pod deleted, and the election runs again. That is safe
because the role label only flips after a successful promotion, so no
client reached it. If no successor appears within `abortFailoverAfter`
(1 min), the failover is aborted and the old primary, which still holds
the newest data and was never replaced, is brought back.

**Reconfiguring.** The new primary gets replication slots for the
others, then every standby gets the new upstream. A checkpoint records
the new timeline for `pg_rewind`, labels and the Lease are updated, and
the phase returns to Healthy.

### Detecting a partition the operator cannot see

The operator's SQL probe only proves that the *operator* can reach the
primary. A partition that cuts the primary off from its standbys and
clients but not from the operator would otherwise go unnoticed while
every write blocks waiting for a synchronous standby. So the standbys are
witnesses. The primary counts as isolated when all of these hold:

- it sees no streaming standby;
- a quorum of standbys is reachable and each reports its WAL receiver down
  or silent for longer than `wal_receiver_timeout`;
- this operator process has seen each of those standbys stream from this
  primary since both last started.

The last condition is kept in memory and tied to each instance's
*incarnation* (pod UID plus container restart count), never read back
from status. After a whole-cluster restart the primary is up before its
standbys reconnect. Stale status would make that look exactly like a
partition. That happened: see [failure-modes.md](failure-modes.md).

## The instance script

Each pod runs `instance.sh` (embedded in the operator binary and mounted
from a ConfigMap) as its init container and its main process. It decides
how to boot from the pod's *current* role label, read through the
downward API rather than the env the pod was created with, because a
standby that was promoted and then had its node rebooted must not come
back as the replica it was created as.

- A **primary** with an empty volume runs `initdb` only if
  `ALLOW_INITDB` is set, which happens on the bootstrap pod alone. A
  primary pod that finds its volume empty for any other reason fails
  loudly instead of starting a new, empty database under the old name.
- A **standby** looks for a writable primary (`-rw` Service first, then
  the creation-time primary) and rewinds against it if needed. If none
  answers, it starts untouched as a standby. It never rewinds against
  nothing and never moves its data aside on a guess.
- A former primary is always recreated with `role=replica`, so it can
  only return through `pg_rewind` or a fresh clone. On boot it refuses to
  start as a primary (self-fence) unless the label says so.

Standbys that cannot follow the primary are recreated by the operator
(`recreateDivergedStandbys`). That covers a standby on an old timeline for
longer than `divergedAfter`, and one that is running but unreachable or
not streaming for longer than `stuckAfter`. The first recreation rewinds.
From the second on (counted in a PVC annotation), the pod is created with
`RECLONE` and the data directory is set aside and cloned fresh. This
covers the case `pg_rewind` cannot: a standby that holds WAL the primary
lost, on the same timeline, for which `pg_rewind` reports "no rewind
required".

## Planned maintenance

- **Drain.** A cordon on the primary's node triggers a switchover before
  the drain gets to the primary, whose PodDisruptionBudget holds the
  eviction until it is relabelled. The upgrade drill
  (`bench/upgrade-drill`) runs the whole k3s minor upgrade through this
  path under the checker.
- **Rolling update.** Out-of-date standbys are recreated one at a time,
  only while every instance is ready. The primary goes last, by
  switchover.

## Observability

The manager exports `pg_operator_failovers_total` (by reason) and
`pg_operator_failover_duration_seconds`, the time from leaving Healthy to
Healthy again as the operator sees it. `.status` carries the phase, the
epoch, the current and target primary and the last failover's reason and
duration. Events mark every step: `Bootstrapped`, `PrimaryFailed`,
`Fenced`, `Promoted`, `FailoverCompleted`, `FailoverAborted` and
`StandbyDiverged`.

## Testing

| Layer | What it proves | How |
|---|---|---|
| Unit | election math, settings rendering, webhook | `make test` |
| envtest | controller state machine against a fake PostgreSQL | `make test` |
| integration | instance script against real PostgreSQL containers | `go test -tags=integration ./internal/pg/` |
| e2e | bootstrap, admission, routing, failover and switchover on k3d | `make test-e2e` |
| chaos | primary kills and partitions under the checker, ≥100 each | `bench/run_scenario.sh` |
| comparison | the same faults against CloudNativePG | `bench/cnpg/` |
| upgrade | k3s 1.36 → 1.37 node by node under the checker | `make upgrade-drill` |

The checker (`test/checker`) runs concurrent reads, writes and
compare-and-set operations against the `-rw` Service. It records every
operation with its real-time interval, including those whose outcome is
unknown, and checks the history for linearizability with Porcupine. It
also reports writes that were acknowledged and later missing (lost) or
present without ever being acknowledged (phantom), and the widest gap
between two acknowledged writes. That gap is the client-observed
unavailability used as the headline number in the benchmark.
