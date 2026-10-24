#!/bin/sh
# Partition the current primary from its standbys and from the checker
# for a while, without Chaos Mesh.
#
#   test/chaos/partition.sh NAMESPACE CLUSTER DURATION_SECONDS
#
# PRIMARY_SELECTOR and REPLICA_SELECTOR (kubectl -l syntax) pick the pods;
# they default to pg-operator's labels for CLUSTER.
#
# Same fault as partition.yaml. Chaos Mesh implements partition with a
# list:set ipset, and the stock WSL2 kernel is built without
# CONFIG_IP_SET_LIST_SET, so there the NetworkChaos objects never inject.
#
# This adds blackhole routes for the peers inside the primary pod's
# network namespace instead, entered from the k3d node container that
# runs it. Every packet the primary sends to a peer is dropped, so no TCP
# exchange in either direction completes: connections stall exactly as
# in a partition. (The primary's own sends fail at once with "invalid
# argument" rather than timing out; its peers only see silence.) It
# deliberately avoids netfilter: the partitioned primary is usually fenced
# and deleted while the fault is in place, and a run that tore down a
# namespace still holding custom iptables chains ended with the WSL2 VM
# dying. Routes go away with the namespace. The operator, in another
# namespace, can still reach the primary.
set -eu

ns=$1
cluster=$2
seconds=$3
primary_selector=${PRIMARY_SELECTOR:-pg.platform.local/cluster=$cluster,pg.platform.local/role=primary}
replica_selector=${REPLICA_SELECTOR:-pg.platform.local/cluster=$cluster,pg.platform.local/role=replica}

primary=$(kubectl -n "$ns" get pod -l "$primary_selector" -o jsonpath='{.items[0].metadata.name}')
node=$(kubectl -n "$ns" get pod "$primary" -o jsonpath='{.spec.nodeName}')
cid=$(kubectl -n "$ns" get pod "$primary" -o jsonpath='{.status.containerStatuses[?(@.name=="postgres")].containerID}')
pid=$(docker exec "$node" crictl inspect --output go-template --template '{{.info.pid}}' "${cid#containerd://}")
peers=$(kubectl -n "$ns" get pod -l "$replica_selector" -o jsonpath='{.items[*].status.podIP}')
peers="$peers $(kubectl -n "$ns" get pod -l app.kubernetes.io/name=pg-checker -o jsonpath='{.items[*].status.podIP}')"

route() { docker exec "$node" nsenter -t "$pid" -n ip route "$@"; }

heal() {
	# The pod may be gone already (fencing deletes it); nothing to undo then.
	for ip in $peers; do
		route del blackhole "$ip/32" 2>/dev/null || true
	done
	echo "partition of $primary healed"
}
trap heal EXIT INT TERM

for ip in $peers; do
	route add blackhole "$ip/32"
done
echo "partitioned $primary on $node from:$peers for ${seconds}s"
sleep "$seconds"
