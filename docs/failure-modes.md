# Failure modes

What pg-operator does under each fault, what the client sees, and the
bugs that fault injection found along the way. Measured numbers live in
[results.md](results.md) and `metrics.json`; this file is about behaviour.

## Faults the operator handles

| Fault | Detected by | Reaction | Client sees |
|---|---|---|---|
| Primary pod deleted or killed | pod missing or terminating (immediate) | fence, elect, promote | writes fail for a few seconds, then succeed on the new primary |
| Primary postgres crashes, pod stays | K failed probes | same | same, plus K × `probeInterval` |
| Primary isolated from standbys and clients, operator can still reach it | standby witnesses (see design.md) | fence over SQL, elect, promote | writes hang until `wal_receiver_timeout` and K rounds, then fail over |
| Primary's node cordoned or drained | `Unschedulable` on the node | switchover with graceful shutdown | a short write gap; no failed transaction is lost |
| Standby killed | pod missing | recreated on its volume, catches up | nothing, if the quorum still has a member |
| Standby on an old timeline after a failover | timeline, for `divergedAfter` | recreated, rejoins via `pg_rewind` | nothing |
| Standby stuck: unreachable, not streaming, or crash-looping | for `stuckAfter` | recreated; rewind first, reclone after | nothing |
| Operator restarts mid-failover | — | resumes the persisted phase | the failover takes longer |
| Whole cluster restarts (all nodes) | — | waits for a quorum; never promotes on partial evidence | unavailable until the instances are back |

## Faults it deliberately does not mask

- **Fewer than `m-n+1` standbys reachable when the primary fails.** No
  promotion: the missing standby may hold the only copy of the last
  acknowledged commit. The cluster stays unavailable for writes until
  enough standbys return. The alternative silently loses data.
- **The only synchronous standby is down while the primary is fine.**
  Writes block (synchronous replication with required durability). The
  operator recreates the standby; it does not relax the quorum.
- **The primary's volume is lost.** A primary pod that boots on an
  empty volume refuses to `initdb`. Recovering it means a failover to a
  standby or a restore, not a fresh empty database under the old name.

## Bugs found by the chaos and restart runs

Each was reproduced, fixed and re-run. They are listed because the
interesting part of an operator is how it behaves when several things go
wrong at once, and the single-fault tests passed all along.

### Whole-cluster restart (host reboot)

A host crash restarted all three k3d nodes at once. The first attempts to
recover found five separate problems:

1. **A standby rewound against a dead primary.** On boot it could not
   reach the primary, treated that as divergence and moved its data
   aside. Now a standby with no reachable primary starts untouched and
   rewinds only against a primary that answers.
2. **The operator fenced a primary nobody could replace.** The standbys
   were still starting, so the primary looked unhealthy, but there was
   nothing to elect. Fencing turned a slow restart into an outage. Now
   failover does not start without an electable standby, and a failover
   that cannot finish within `abortFailoverAfter` brings the old primary
   back.
3. **A recreated primary pod could `initdb` an empty volume.** Now only
   the bootstrap pod carries `ALLOW_INITDB`.
4. **A false partition.** The primary came up first, saw no standbys and
   the status from before the restart said they had been streaming, which
   looked exactly like isolation. Witness evidence is now kept in memory
   and tied to each instance's incarnation.
5. **A promotion target that never became reachable.** A hot standby
   that restarts while no primary runs never accepts connections, because
   it needs a running-transactions record that only a primary writes.
   Waiting for it blocked the failover for good. A stuck target is now
   abandoned and its pod deleted, and the election runs again.

### A standby ahead of its primary on the same timeline

After a VM crash, one standby held WAL that the primary had lost. Both
were on the same timeline, the standby's LSN was past the primary's, and
streaming failed. `pg_rewind` reports "no rewind required" in that case
because it compares timelines, so a rewind never fixes it. Standbys that
stay stuck are now recreated with a rejoin counter on their PVC: the
first attempt rewinds, the second sets the data directory aside and
reclones. The likely cause is that an fsync inside a WSL2 VM is not
durable across a crash of the VM. That is a property of the test host,
but the same shape (a replica with WAL its primary lost) appears on real
hardware with a lying disk cache, so the operator has to handle it.

### A standby that crash-loops on a damaged data directory

After the cluster's nodes were stopped abruptly, one standby came back
with a torn checkpoint record and PostgreSQL panicked on every start
("could not locate a valid checkpoint record"). The stuck-standby check
only looked at containers in the Running state, and a crash-looping one
is mostly waiting in CrashLoopBackOff, so it was never recreated. It sat
there for over fifteen minutes with the cluster down to one standby.
Any container that has started at least once now counts. After
`stuckAfter` the standby is recreated, and since one rejoin had already
failed it was recloned and streaming again within a minute.

### Freeze before the election

A primary killed with grace 0 is not dead the instant its pod object is
gone. Its preStop fast shutdown can still ship the shutdown checkpoint to
one standby after the election sampled LSNs. That standby then sits one
record past the new timeline's fork point and can never follow it. The
election now clears every standby's `primary_conninfo` first and
re-observes.

### A drain that could never evict a standby

The standby PodDisruptionBudget used `maxUnavailable: 1`. The disruption
controller sizes such a budget from the pods' controller owner, and the
PostgresCluster's scale subresource counts every instance, primary
included: it expected three pods, found two healthy standbys and allowed
zero disruptions. Every drain therefore stalled on a standby. Unit tests
and the cordon e2e scenario never drained a standby; the k3s upgrade
drill did, and hung. The budget now uses an absolute `minAvailable`.

### Measuring through a rolling node restart

On k3d, upgrading a node restarts its k3s, which restarts the node
container and every pod on it. The first drill ran the checker as a Job
in the cluster, so a drain evicted it and its history was lost. The drill
now runs the checker outside the cluster. Its eight-minute history then
pushed Porcupine's verbose mode past the checker's memory limit after
the workload finished; the verbose pass now runs only on a key that
fails.

### Partition injection

Chaos Mesh implements `NetworkChaos` partitions with a `list:set` ipset.
The stock WSL2 kernel lacks `CONFIG_IP_SET_LIST_SET`, so the experiment is
accepted, reports an injection error in its status, and never cuts
anything. A checker run against it passes, vacuously. The benchmark
therefore verifies every injection before counting the result.

The first replacement used iptables chains inside the primary's network
namespace. One such run ended with the entire WSL2 VM dying, most
likely when the partitioned primary was fenced and its namespace torn
down with the chains still in place. No kernel log survived to prove it.
`test/chaos/partition.sh` now uses blackhole routes in the namespace,
which disappear with it.

## Limits of the test environment

- **Node failure detection is slow.** `node-monitor-grace-period` is 50 s
  in Kubernetes 1.37, and pods on an unreachable node are only evicted
  after the 300 s `unreachable` toleration. pg-operator does not depend on
  either (it probes PostgreSQL directly), but a test that kills a whole
  node measures those timers for any component that does.
- **No latency or loss injection.** The WSL2 kernel has no `sch_netem`,
  so `NetworkChaos` `delay`, `loss` and `corrupt` fail there. Slow-network
  behaviour is untested on this host.
- **Single host.** All nodes share one machine, one disk and one kernel.
  Faults are process and network faults; disk and power faults of a
  single node are not modelled.
