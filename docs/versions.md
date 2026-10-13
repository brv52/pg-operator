# Pinned versions

Resolved on 2026-10-24 against upstream registries and GitHub releases with
`gh release list`, `go list -m -versions` and `crane digest`. Re-run
`hack/preflight.sh` to reproduce the check.

## Operator toolchain

| Component | Pinned | Resolved | Notes |
|---|---|---|---|
| Go | 1.27 | `go1.27.0` | `go.mod` uses `go 1.27.0`; Dockerfile builds on `golang:1.27` |
| Kubebuilder | 4.16.0 | `v4.16.0` | Scaffolds controller-runtime v0.25.0 and k8s.io/* v0.37.0 |
| controller-runtime | matching release | `v0.25.2` | Latest patch on the v0.25 line that Kubebuilder 4.16 targets |
| k8s.io/{api,apimachinery,client-go} | 1.37 | `v0.37.1` | |
| controller-gen | — | `v0.22.0` | From the Kubebuilder Makefile |
| envtest assets | 1.37 | `1.37.x` | Resolved by `setup-envtest` from the k8s.io/api minor |

## Clusters

| Component | Pinned | Resolved | Digest |
|---|---|---|---|
| k3d | — | `v5.9.0` | |
| k3s (e2e, chaos, benchmark) | Kubernetes 1.37 | `rancher/k3s:v1.37.1-k3s1` | `sha256:ca7f37d993d82ef0dcdcfecb2e0e2618ea541dbaffc620c8cedebe01a82acd0d` |
| k3s (upgrade drill, from) | v1.36.x | `rancher/k3s:v1.36.5-k3s1` | `sha256:057cfd5a9d51b89f1f0ee139c28d5d5fdd5aa7ddeac2742a0ba90c6dbc0329bb` |
| k3s (upgrade drill, to) | v1.37.x | `rancher/k3s:v1.37.1-k3s1` | `sha256:ca7f37d993d82ef0dcdcfecb2e0e2618ea541dbaffc620c8cedebe01a82acd0d` |
| system-upgrade-controller | latest | `v0.20.2` | `sha256:61b68d4372baf7ae90fb0f879feea1c082381a8b7547cc5ec85d393c497b11cb` |

All local clusters are k3d (k3s in containers): one server and two agents,
Kubernetes `v1.37.1`. kind was the first choice, but a 4-node kind cluster
plus cert-manager, Chaos Mesh and two operators does not fit an 8 GB WSL2
VM; k3s gives the same three real nodes, containerd runtime and Kubernetes
API for roughly a third of the memory, and is also the distribution the
upgrade drill (k3s 1.36.5 → 1.37.1 with system-upgrade-controller) needs.

cert-manager is not used: the operator provisions its own webhook
certificate (`internal/webhookcert`).

## Operand

| Component | Pinned | Resolved | Digest |
|---|---|---|---|
| PostgreSQL | current major | `postgres:18.6-bookworm` (`PG_VERSION=18.6-1.pgdg12+2`) | `sha256:bbf83594798e144bff365a9c191c5d779f181fb3135902064ab8ff11918020fc` |

The official Debian image ships `pg_ctl`, `pg_rewind`, `pg_basebackup`,
`pg_controldata` and `pg_isready` under `/usr/lib/postgresql/18/bin`, and
runs as `postgres` (uid/gid 999). The distroless option was dropped because
the instance bootstrap script needs a POSIX shell. The same digest is used
for both pg-operator and the CloudNativePG comparator so that the operand
is not a variable in the benchmark.

## Chaos, checker and comparator

| Component | Pinned | Resolved | Digest / notes |
|---|---|---|---|
| Chaos Mesh | 2.8.5 | `v2.8.5` | `ghcr.io/chaos-mesh/chaos-mesh:v2.8.5` `sha256:942bff26d591537f655df50532e3880d73fa8b6fca34a233fc69f8ccfd36188d`. v2.8.6 shipped 2026-10-21; the pin is kept to 2.8.5 as specified. |
| chaos-daemon | 2.8.5 | `v2.8.5` | `sha256:d794bf427ab1dfacfd51c2f417d26731ce46919637b1c83620f0afd40f580bd4` |
| Porcupine | v1.3.1 | `v1.3.1` | Go module |
| Elle | 0.2.8 | `0.2.8` | Optional offline analysis of the exported EDN history |
| CloudNativePG | 1.30.1 | `v1.30.1` | `ghcr.io/cloudnative-pg/cloudnative-pg:1.30.1` `sha256:923c267ec29636db3bee20f993d0ec4973fa22998e1adad37da79e4d32b5bc07` |
| kubent | latest | `0.7.3` | Last release is from 2024-08; it still covers the removed-API rulesets up to 1.32 only, so pluto is the authoritative scanner for 1.37 |
| pluto | latest | `v5.24.4` | |
| pgx | — | `v5.11.0` | SQL driver for the operator and checker |

## Kernel requirements (WSL2 host)

Checked on `5.15.167.4-microsoft-standard-WSL2`:

| Option | State | Impact |
|---|---|---|
| `CONFIG_IP_SET` | `y` | Required by `NetworkChaos` `partition` (ipset + iptables). OK. |
| `CONFIG_NETFILTER_XT_SET` | `y` | Same. OK. |
| `CONFIG_NET_SCH_NETEM` | not set | `NetworkChaos` `delay`/`loss`/`corrupt` will fail. Not used by the benchmark scenarios; required only for optional latency experiments (custom WSL kernel). |

## Kubernetes behaviour that affects the results

`node-monitor-grace-period` defaults to **50s** in 1.37
(`pkg/controller/nodelifecycle/config/v1alpha1/defaults.go`); it was 40s up
to 1.31. Any failover that waits for the node to be marked `NotReady` has a
floor of roughly 50s, and pod eviction from an unreachable node adds the
`node.kubernetes.io/unreachable` toleration (300s by default). pg-operator
detects primary loss from SQL probes and standby WAL-receiver state, so it
does not wait for either.
