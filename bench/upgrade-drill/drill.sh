#!/usr/bin/env bash
# Kubernetes minor upgrade drill: start a fresh three-node k3s cluster on
# the previous minor, scan it for removed APIs, run a pg-operator cluster
# on it under the linearizability checker, upgrade every node with
# system-upgrade-controller and record what the client saw.
#
#   bench/upgrade-drill/drill.sh     (or make upgrade-drill)
#
# Writes bench/upgrade-drill/result.json and the scanner reports next to
# it; bench/aggregate copies the result into metrics.json. The drill
# cluster (pg-drill) is deleted afterwards unless KEEP=1.
#
# It needs the memory of a whole cluster, so it refuses to start while
# another k3d cluster is running: stop the main one first with
# "k3d cluster stop pg-operator" and start it again afterwards.
set -euo pipefail

from_image=rancher/k3s:v1.36.5-k3s1@sha256:057cfd5a9d51b89f1f0ee139c28d5d5fdd5aa7ddeac2742a0ba90c6dbc0329bb
to_version=v1.37.1+k3s1
suc_version=v0.20.2
name=pg-drill
checker_duration=${CHECKER_DURATION:-8m}
upgrade_timeout=${UPGRADE_TIMEOUT:-1200}

root=$(cd "$(dirname "$0")/../.." && pwd)
dir=$root/bench/upgrade-drill
bin=$root/bin
cd "$root"

log() { echo "$(date -u +%H:%M:%S) [drill] $*"; }

wait_healthy() {
	local deadline=$((SECONDS + 600))
	until [ "$(kubectl -n demo get pgc demo -o jsonpath='{.status.failoverPhase}/{.status.readyInstances}' 2>/dev/null)" = Healthy/3 ]; do
		[ "$SECONDS" -ge "$deadline" ] && return 1
		sleep 5
	done
}

running=$(k3d cluster list -o json | jq -r --arg n "$name" \
	'.[] | select(.name != $n and .serversRunning > 0) | .name')
if [ -n "$running" ]; then
	echo "k3d cluster(s) running: $running; stop them first (k3d cluster stop NAME)" >&2
	exit 1
fi

if ! k3d cluster list "$name" >/dev/null 2>&1; then
	log "creating $name on $from_image"
	k3d cluster create "$name" --config hack/k3d-config.yaml --image "$from_image"
	# Same swap cap as make cluster-up.
	for n in $(k3d node list -o json | jq -r --arg c "$name" \
		'.[] | select(.runtimeLabels["k3d.cluster"] == $c and .role != "loadbalancer") | .name'); do
		docker update --memory-swap "$(docker inspect -f '{{.HostConfig.Memory}}' "$n")" "$n" >/dev/null
	done
fi
kubectl config use-context "k3d-$name" >/dev/null
checker=$name-checker
cleanup() {
	docker rm -f "$checker" >/dev/null 2>&1 || true
	if [ "${KEEP:-0}" != 1 ]; then
		log "deleting $name"
		k3d cluster delete "$name" >/dev/null
	fi
}
trap cleanup EXIT
from=$(kubectl version -o json | jq -r .serverVersion.gitVersion)

log "deploying pg-operator"
make -s deploy-local K3D_CLUSTER="$name" >/dev/null
kubectl create namespace demo --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n demo apply -f config/samples/pg_v1alpha1_postgrescluster.yaml >/dev/null
wait_healthy

log "scanning for APIs removed in ${to_version%%+*}"
# pluto checks both what is deployed and what the repository would
# deploy; kubent's rulesets stop at 1.32, so it is a second opinion only.
target=${to_version%%+*}
"$bin/pluto" detect-all-in-cluster --target-versions "k8s=$target" -o wide >"$dir/pluto-cluster.txt" 2>&1 || true
"$bin/pluto" detect-files -d bin/deploy-local.yaml --target-versions "k8s=$target" -o wide >"$dir/pluto-files.txt" 2>&1 || true
"$bin/kubent" --target-version "${target#v}" -o text 2>&1 | sed 's/\x1b\[[0-9;]*m//g' >"$dir/kubent.txt" || true
removed=$("$bin/pluto" detect-all-in-cluster --target-versions "k8s=$target" -o json 2>/dev/null |
	jq '[.items[]? | select(.removed)] | length' || echo null)

log "installing system-upgrade-controller $suc_version"
suc=https://github.com/rancher/system-upgrade-controller/releases/download/$suc_version
kubectl apply --server-side -f "$suc/crd.yaml" -f "$suc/system-upgrade-controller.yaml" >/dev/null
kubectl -n system-upgrade rollout status deployment/system-upgrade-controller --timeout=5m

epoch0=$(kubectl -n demo get pgc demo -o jsonpath='{.status.epoch}')
# The checker runs outside the cluster, as a client would. Upgrading a
# node restarts k3s, and on k3d that restarts the node container and every
# pod on it, so an in-cluster checker cannot survive a full rolling
# upgrade (its history would be lost with its pod). Instead it connects to
# the primary through a NodePort on all three nodes, and libpq-style
# multi-host failover with target_session_attrs=read-write skips a node
# that is restarting.
kubectl -n demo create service nodeport demo-rw-external --tcp=5432:5432 --node-port=30432 \
	--dry-run=client -o json |
	jq '.spec.selector = {"pg.platform.local/cluster": "demo", "pg.platform.local/role": "primary"}' |
	kubectl apply -f - >/dev/null
hosts=$(k3d node list -o json | jq -r --arg c "$name" \
	'[.[] | select(.runtimeLabels["k3d.cluster"] == $c and .role != "loadbalancer") | .name] | join(",")')
password=$(kubectl -n demo get secret demo-credentials -o jsonpath='{.data.superuser-password}' | base64 -d)
log "starting checker for $checker_duration against $hosts"
docker rm -f "$checker" >/dev/null 2>&1 || true
rm -rf "$dir/checker" && mkdir -p "$dir/checker" && chmod 777 "$dir/checker"
docker run -d --name "$checker" --network "k3d-$name" --memory 512m -v "$dir/checker:/out" \
	-e PGPASSWORD="$password" \
	-e CHECKER_DSN="host=$hosts port=30432 user=postgres dbname=postgres connect_timeout=2 sslmode=disable target_session_attrs=read-write" \
	pg-checker:dev -duration="$checker_duration" -key-base=$(($(date +%s%N) / 1000)) -workers=4 -keys=4 -out=/out >/dev/null
sleep 20

log "upgrading to $to_version"
started=$SECONDS
TO_VERSION=$to_version envsubst <"$dir/plans.yaml" | kubectl apply -f - >/dev/null
upgraded=false
deadline=$((SECONDS + upgrade_timeout))
while [ "$SECONDS" -lt "$deadline" ]; do
	# The API server goes away while the server node restarts.
	versions=$(kubectl get nodes -o jsonpath='{range .items[*]}{.status.nodeInfo.kubeletVersion}{" "}{.spec.unschedulable}{"\n"}{end}' 2>/dev/null || true)
	if [ -n "$versions" ] && ! grep -qv "^$to_version $" <<<"$versions"; then
		upgraded=true
		break
	fi
	sleep 10
done
upgrade_seconds=$((SECONDS - started))
log "nodes upgraded=$upgraded after ${upgrade_seconds}s"

recovered=true
wait_healthy || recovered=false
# The checker runs for its full duration; a drill that outlasts it only
# partly measured the upgrade.
covered=$(docker inspect -f '{{.State.Running}}' "$checker")
docker wait "$checker" >/dev/null
docker logs "$checker" >"$dir/checker/output.log" 2>&1 || true
log "checker exited $(docker inspect -f '{{.State.ExitCode}} oom={{.State.OOMKilled}}' "$checker")"
result=$(cat "$dir/checker/result.json" 2>/dev/null || true)
jq -e . >/dev/null 2>&1 <<<"$result" || result=null
epoch1=$(kubectl -n demo get pgc demo -o jsonpath='{.status.epoch}')

jq -n --arg from "$from" --arg to "$to_version" --argjson upgraded "$upgraded" \
	--argjson upgradeSeconds "$upgrade_seconds" --argjson recovered "$recovered" \
	--argjson covered "$covered" --argjson primaryChanges "$((epoch1 - epoch0))" \
	--argjson removedAPIs "$removed" --argjson checker "$result" \
	--arg date "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
	'{date: $date, from: $from, to: $to, nodesUpgraded: $upgraded, upgradeSeconds: $upgradeSeconds,
	  clusterRecovered: $recovered, checkerCoveredUpgrade: $covered, primaryChanges: $primaryChanges,
	  removedAPIsInUse: $removedAPIs, passed: ($upgraded and $recovered and ($checker.passed // false)),
	  checker: $checker}' >"$dir/result.json"
log "result: $(jq -c '{passed, upgradeSeconds, primaryChanges, gap: .checker.maxWriteGapMs}' "$dir/result.json")"
