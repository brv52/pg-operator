# Results

Every number in this file comes from `metrics.json`, which
`make metrics` aggregates from the per-fault results that
`bench/run_scenario.sh` writes. A value that no run has produced yet is
`null` there and "not yet run" here; nothing is estimated.

## Method

- **Cluster.** k3d with one k3s v1.37.1 server and two agents (one
  instance per node), memory-capped, on a single WSL2 host. Both
  operators run the same PostgreSQL 18.6 image digest with three
  instances, an `ANY 1` synchronous quorum, 32 MB `shared_buffers` and
  256 Mi memory limits. Only one operator's cluster runs at a time.
- **Workload.** The checker Job runs for 60 s per fault: four workers
  doing reads, writes and compare-and-set operations on four keys through
  the `-rw` Service, each operation bounded at 5 s including the connect.
  Faults are injected 15 s in.
- **Faults.** `podkill`: the current primary's pod is killed with grace
  0 by Chaos Mesh. `partition`: the primary is cut off from its standbys
  and from the checker for 30 s by blackhole routes in its network
  namespace (`test/chaos/partition.sh`, see failure-modes.md for why not
  Chaos Mesh); the operator can still reach it.
- **Validity.** Each fault is verified (the primary's pod UID changed,
  or the routes went in). Faults that did not inject are counted but
  excluded from every statistic.
- **Headline metric.** The widest gap between two acknowledged writes
  during the run, as measured by the client. It covers detection,
  failover and the client reconnecting, and it means the same thing for
  both operators, unlike either operator's own failover timer.

## Primary pod kill

| | pg-operator | CloudNativePG 1.30.1 |
|---|---|---|
| faults (injected) | not yet run | not yet run |
| write gap p50 / p99 / max | not yet run | not yet run |
| linearizable runs | not yet run | not yet run |
| lost / phantom writes | not yet run | not yet run |

## Partition of the primary

| | pg-operator | CloudNativePG 1.30.1 |
|---|---|---|
| faults (injected) | not yet run | not yet run |
| write gap p50 / p99 / max | not yet run | not yet run |
| linearizable runs | not yet run | not yet run |
| lost / phantom writes | not yet run | not yet run |

## Kubernetes upgrade drill

k3s v1.36.5 → v1.37.1 with system-upgrade-controller, one node at a
time, each cordoned and drained, under the checker
(`bench/upgrade-drill`).

| | |
|---|---|
| nodes upgraded | not yet run |
| primary changes | not yet run |
| write gap max | not yet run |
| linearizable, lost writes | not yet run |
| removed APIs in use (pluto) | not yet run |

## Reproducing

```sh
make cluster-up deploy-local chaos-install
kubectl create namespace demo
kubectl -n demo apply -f config/samples/pg_v1alpha1_postgrescluster.yaml
bench/run_scenario.sh podkill 100
bench/run_scenario.sh partition 100

kubectl -n demo delete pgc demo
bench/cnpg/install.sh up
OPERATOR=cnpg NAMESPACE=cnpg-bench CLUSTER=bench bench/run_scenario.sh podkill 100
OPERATOR=cnpg NAMESPACE=cnpg-bench CLUSTER=bench bench/run_scenario.sh partition 100
bench/cnpg/install.sh down

k3d cluster stop pg-operator && make upgrade-drill && k3d cluster start pg-operator
make metrics
```

A run that is interrupted continues where it stopped when the same
command is run again.
