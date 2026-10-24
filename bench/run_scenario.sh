#!/usr/bin/env bash
# Inject one kind of fault into a 3-instance PostgreSQL cluster again and
# again while the linearizability checker runs against it, and record one
# result per fault.
#
#   bench/run_scenario.sh SCENARIO ITERATIONS
#
#   SCENARIO   podkill | partition
#   OPERATOR   pgop (default) | cnpg: which operator's labels, Services and
#              Secret to use
#   NAMESPACE, CLUSTER   the cluster under test (default demo/demo)
#   OUT        results directory (default bench/results/$OPERATOR-$SCENARIO)
#
# Each iteration waits for the cluster to be healthy, starts a checker Job
# (CHECKER_DURATION, default 60s), injects the fault FAULT_AT seconds in
# (default 15), verifies it really took effect, waits for the checker and
# for the cluster to recover, and writes $OUT/NNN.json. Iterations whose
# file exists are skipped, so an interrupted run is resumed by running the
# same command again. bench/aggregate turns the files into metrics.json.
#
# The run stops instead of carrying on when the cluster does not recover,
# and pauses while the host is short of memory.
set -euo pipefail

scenario=${1:?scenario: podkill | partition}
iterations=${2:?iterations}
operator=${OPERATOR:-pgop}
ns=${NAMESPACE:-demo}
cluster=${CLUSTER:-demo}
out=${OUT:-bench/results/$operator-$scenario}
checker_duration=${CHECKER_DURATION:-60s}
fault_at=${FAULT_AT:-15}
partition_seconds=${PARTITION_SECONDS:-30}
instances=${INSTANCES:-3}
recover_timeout=${RECOVER_TIMEOUT:-600}

case $operator in
pgop)
	cluster_sel="pg.platform.local/cluster=$cluster"
	primary_sel="$cluster_sel,pg.platform.local/role=primary"
	replica_sel="$cluster_sel,pg.platform.local/role=replica"
	primary_labels="{\"pg.platform.local/cluster\": \"$cluster\", \"pg.platform.local/role\": \"primary\"}"
	secret=$cluster-credentials secret_key=superuser-password
	;;
cnpg)
	cluster_sel="cnpg.io/cluster=$cluster"
	primary_sel="$cluster_sel,cnpg.io/instanceRole=primary"
	replica_sel="$cluster_sel,cnpg.io/instanceRole=replica"
	primary_labels="{\"cnpg.io/cluster\": \"$cluster\", \"cnpg.io/instanceRole\": \"primary\"}"
	secret=$cluster-superuser secret_key=password
	;;
*)
	echo "unknown OPERATOR $operator" >&2
	exit 2
	;;
esac
rw_service=$cluster-rw
root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$out"

log() { echo "$(date -u +%H:%M:%S) [$operator/$scenario] $*"; }

# healthy: every instance Ready, none terminating, exactly one primary.
healthy() {
	local rows ready=0 primaries
	rows=$(kubectl -n "$ns" get pod -l "$cluster_sel" -o \
		jsonpath='{range .items[*]}{.metadata.deletionTimestamp}{"|"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}')
	while IFS='|' read -r deleting isready; do
		[ -z "$deleting" ] && [ "$isready" = True ] && ready=$((ready + 1))
	done <<<"$rows"
	primaries=$(kubectl -n "$ns" get pod -l "$primary_sel" --no-headers 2>/dev/null | grep -c ' Running ' || true)
	[ "$ready" -eq "$instances" ] && [ "$primaries" -eq 1 ]
}

wait_healthy() {
	local deadline=$((SECONDS + $1))
	until healthy; do
		[ "$SECONDS" -ge "$deadline" ] && return 1
		sleep 2
	done
}

# memory_ok: enough headroom in the VM and, under WSL2, on the Windows
# side, whose commit limit is what actually runs out.
memory_ok() {
	local avail win
	avail=$(awk '/MemAvailable/ {print int($2/1024)}' /proc/meminfo)
	[ "$avail" -ge "${MIN_AVAILABLE_MB:-800}" ] || return 1
	if command -v powershell.exe >/dev/null; then
		win=$(powershell.exe -NoProfile -Command \
			'[int]((Get-CimInstance Win32_OperatingSystem).FreeVirtualMemory/1KB)' 2>/dev/null | tr -d '\r')
		[ -z "$win" ] || [ "$win" -ge "${MIN_WINDOWS_COMMIT_MB:-2000}" ] || return 1
	fi
}

primary_pod() {
	kubectl -n "$ns" get pod -l "$primary_sel" \
		-o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.uid}{"\n"}{end}' | head -1
}

cleanup() {
	kubectl -n "$ns" delete job,podchaos,networkchaos -l "pg.platform.local/run=$run" \
		--ignore-not-found --wait=false >/dev/null 2>&1 || true
}

for ((i = 1; i <= iterations; i++)); do
	file=$(printf '%s/%03d.json' "$out" "$i")
	[ -s "$file" ] && continue
	run=$(printf '%s-%s-%03d-%s' "$operator" "$scenario" "$i" "$(date +%s | tail -c 5)")
	trap cleanup EXIT

	until memory_ok; do
		log "host memory is low, waiting"
		sleep 30
	done
	if ! wait_healthy "$recover_timeout"; then
		log "cluster not healthy before iteration $i, stopping"
		exit 1
	fi

	log "iteration $i: starting checker $run"
	NAMESPACE=$ns SECRET=$secret SECRET_KEY=$secret_key RW_SERVICE=$rw_service RUN_ID=$run \
		DURATION=$checker_duration KEY_BASE=$(($(date +%s%N) / 1000)) \
		envsubst <"$root/test/checker/job.yaml" | kubectl apply -f - >/dev/null
	kubectl -n "$ns" wait --for=condition=Ready pod -l "pg.platform.local/run=$run" --timeout=120s >/dev/null
	started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
	sleep "$fault_at"

	read -r before before_uid <<<"$(primary_pod)"
	fault_start=$SECONDS
	injected=false
	case $scenario in
	podkill)
		NAMESPACE=$ns RUN_ID=$run PRIMARY_LABELS=$primary_labels \
			envsubst <"$root/test/chaos/podkill.yaml" | kubectl apply -f - >/dev/null
		# Injected once the pod that was primary is gone or replaced.
		for _ in $(seq 30); do
			uid=$(kubectl -n "$ns" get pod "$before" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
			if [ "$uid" != "$before_uid" ]; then
				injected=true
				break
			fi
			sleep 0.5
		done
		;;
	partition)
		if PRIMARY_SELECTOR=$primary_sel REPLICA_SELECTOR=$replica_sel \
			"$root/test/chaos/partition.sh" "$ns" "$cluster" "$partition_seconds"; then
			injected=true
		fi
		;;
	*)
		echo "unknown scenario $scenario" >&2
		exit 2
		;;
	esac
	log "iteration $i: fault on $before injected=$injected"

	# Time until every instance is Ready again, measured from the fault
	# while the checker keeps running.
	recovered=true
	wait_healthy "$recover_timeout" || recovered=false
	recovery=$((SECONDS - fault_start))
	read -r after _ <<<"$(primary_pod)"

	status=timeout
	for _ in $(seq 180); do
		s=$(kubectl -n "$ns" get job "checker-$run" -o jsonpath='{.status.succeeded}/{.status.failed}')
		case $s in
		1/*) status=complete && break ;;
		*/1) status=failed && break ;;
		esac
		sleep 2
	done
	checker=$(kubectl -n "$ns" logs "job/checker-$run" --tail=1 2>/dev/null || true)
	echo "$checker" | jq -e . >/dev/null 2>&1 || checker=null

	jq -n --argjson iteration "$i" --arg operator "$operator" --arg scenario "$scenario" \
		--arg started "$started" --arg before "$before" --arg after "${after:-}" \
		--argjson injected "$injected" --arg status "$status" --argjson recovered "$recovered" \
		--argjson recovery "$recovery" --argjson checker "$checker" \
		'{iteration: $iteration, operator: $operator, scenario: $scenario, started: $started,
		  primaryBefore: $before, primaryAfter: $after, injected: $injected,
		  checkerStatus: $status, recovered: $recovered, recoverySeconds: $recovery,
		  checker: $checker}' >"$file.tmp"
	mv "$file.tmp" "$file"
	log "iteration $i: checker=$status passed=$(jq -r '.checker.passed // "n/a"' "$file")" \
		"gap=$(jq -r '.checker.maxWriteGapMs // "n/a"' "$file")ms recovery=${recovery}s"
	cleanup
	trap - EXIT

	if [ "$recovered" != true ]; then
		log "cluster did not recover after iteration $i, stopping"
		exit 1
	fi
done
log "done: $(find "$out" -name '*.json' | wc -l) results in $out"
