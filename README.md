# pg-operator

A Kubernetes operator for PostgreSQL high availability. It runs one
primary and synchronous streaming standbys, and fails over without losing
an acknowledged write. That claim is checked, not assumed: every failover
path runs under fault injection while a
[Porcupine](https://github.com/anishathalye/porcupine) linearizability
checker drives traffic through the cluster, and the same harness runs
against CloudNativePG for comparison.

Go, controller-runtime, Kubebuilder; PostgreSQL 18; tested on k3s 1.37.

## What it does

- **Quorum-safe failover.** Commits wait for `ANY n` of the standbys. On
  failure the operator freezes the standbys, inspects at least `m-n+1` of
  them (which is enough to be sure the most advanced holds every
  acknowledged commit), and promotes that one. If it cannot see enough
  standbys, it does not promote.
- **Fencing before promotion.** The old primary is removed from the `-rw`
  Service, made read-only if reachable, and deleted before anything is
  promoted. A former primary only ever returns as a standby, through
  `pg_rewind` or a fresh clone.
- **Partition detection the API server cannot give you.** A primary cut
  off from its standbys but still reachable by the operator is detected
  from the standbys' side and failed over.
- **Crash-safe state machine.** Healthy → Detecting → Fencing → Promoting
  → Reconfiguring. Each step is persisted before it acts, so the operator
  can be killed at any point and resume.
- **Planned maintenance.** A cordoned node triggers a graceful switchover
  before the drain reaches the primary; rolling updates replace one
  standby at a time and the primary last.
- **Self-healing standbys.** Standbys that cannot follow the primary are
  rewound, then recloned if rewinding does not help.

How and why: [docs/design.md](docs/design.md). What happens under each
fault, including the bugs the chaos runs found:
[docs/failure-modes.md](docs/failure-modes.md).

## Results

From `metrics.json`, which the benchmark writes; see
[docs/results.md](docs/results.md) for the method. The headline number is
the widest gap between two acknowledged writes as the client saw it.

| | pg-operator | CloudNativePG 1.30.1 |
|---|---|---|
| Primary kill, write gap p99 | pending | pending |
| Partition, write gap p99 | pending | — |
| Faults verified by the checker | pending | |
| Lost acknowledged writes | pending | |
| k3s 1.36 → 1.37 rolling upgrade under load | passed: 3 nodes in 114 s, 2 primary changes, 128k ops linearizable, 0 lost writes, widest write gap 5.1 s | |

"Pending" means the long fault runs have not been recorded yet; nothing
here is estimated. The upgrade row is one recorded drill
(`bench/upgrade-drill/result.json`).

## Quick start

Needs Docker, k3d 5.9, kubectl, Go 1.27, and about 4 GB of memory.

```sh
make cluster-up       # k3s: one server, two agents
make deploy-local     # build, import and deploy the operator
kubectl create namespace demo
kubectl -n demo apply -f config/samples/pg_v1alpha1_postgrescluster.yaml
kubectl -n demo get pgc demo -w
```

```yaml
apiVersion: pg.platform.local/v1alpha1
kind: PostgresCluster
metadata:
  name: demo
spec:
  instances: 3
  synchronous: {method: any, number: 1}
  storage: {size: 1Gi}
  failover: {probeInterval: 1s, failureThresholdK: 3}
```

Connect through `demo-rw` (primary) or `demo-ro` (standbys); the
superuser password is in the `demo-credentials` Secret.

Kill the primary and watch it fail over:

```sh
kubectl -n demo delete pod "$(kubectl -n demo get pgc demo -o jsonpath='{.status.currentPrimary}')" \
  --grace-period=0 --force
kubectl -n demo get events -w
```

## Testing

```sh
make test              # unit tests and envtest
make test-integration  # instance script against real PostgreSQL (Docker)
make test-e2e          # five scenarios on k3d, including failover under the checker
make lint
```

Fault injection and benchmarks are in [bench/](bench) and
[test/chaos](test/chaos); [docs/results.md](docs/results.md) lists the
commands. Pinned versions of everything are in
[docs/versions.md](docs/versions.md).

## Layout

| Path | |
|---|---|
| `api/v1alpha1` | `PostgresCluster` types, defaults, admission webhook |
| `internal/controller` | reconciler, failover state machine, convergence |
| `internal/pg` | SQL client, election, settings, the instance script |
| `test/checker` | linearizability checker (workload, model, Porcupine) |
| `test/e2e` | Ginkgo suite on k3d |
| `test/chaos` | pod-kill and partition experiments |
| `bench` | fault runner, aggregator, CloudNativePG comparator, upgrade drill |

## Scope

This is a study of the failover core, not a full PostgreSQL platform.
Backups, point-in-time recovery, major version upgrades and connection
pooling are out of scope; for production use, look at CloudNativePG.

## License

Apache 2.0. See [LICENSE](LICENSE).
