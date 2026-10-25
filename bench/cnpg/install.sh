#!/bin/sh
# Install CloudNativePG and the comparison cluster, or remove both.
#
#   bench/cnpg/install.sh [up|down]
#
# The release manifest is pinned by version and checksum. The operator
# runs with one replica and a memory limit so that it fits next to the
# benchmark on a laptop. Run the CloudNativePG series with no pg-operator
# cluster running and vice versa: the two operators never share the nodes
# during a measurement.
set -eu

version=1.30.1
sha256=37237f145d8138256ea25ae830f87759255665ff08f8d552fdd8224a5ec032fb
url=https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.30/releases/cnpg-$version.yaml
dir=$(cd "$(dirname "$0")" && pwd)

case ${1:-up} in
up)
	manifest=$(mktemp)
	trap 'rm -f "$manifest"' EXIT
	curl -fsSL -o "$manifest" "$url"
	echo "$sha256  $manifest" | sha256sum -c --quiet
	kubectl apply --server-side --force-conflicts -f "$manifest" >/dev/null
	kubectl -n cnpg-system set resources deployment/cnpg-controller-manager \
		--requests=cpu=50m,memory=96Mi --limits=memory=256Mi >/dev/null
	kubectl -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=5m
	kubectl create namespace cnpg-bench --dry-run=client -o yaml | kubectl apply -f - >/dev/null
	kubectl apply -f "$dir/cluster.yaml"
	kubectl -n cnpg-bench wait --for=condition=Ready cluster/bench --timeout=10m
	;;
down)
	# The Cluster goes first: deleting the namespace under a running
	# operator only races it into recreating what it owns.
	kubectl -n cnpg-bench delete cluster bench --ignore-not-found --wait=true --timeout=5m
	kubectl delete namespace cnpg-bench --ignore-not-found --wait=true --timeout=5m
	kubectl delete -f "$url" --ignore-not-found >/dev/null
	;;
*)
	echo "usage: $0 [up|down]" >&2
	exit 2
	;;
esac
