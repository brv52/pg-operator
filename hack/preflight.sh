#!/usr/bin/env bash
# Verifies that the pinned stack in docs/versions.md still resolves upstream
# and that the host kernel can run the Chaos Mesh scenarios.
set -euo pipefail

fail=0
check() {
	local what=$1; shift
	if out=$("$@" 2>&1); then
		printf '  ok    %-48s %s\n' "$what" "$(tail -1 <<<"$out")"
	else
		printf '  FAIL  %-48s %s\n' "$what" "$(tail -1 <<<"$out")"
		fail=1
	fi
}

echo "toolchain"
check "go 1.27"               bash -c 'go version | grep -q "go1.27" && go version'
check "kubebuilder 4.16.0"    bash -c 'kubebuilder version | grep -q "v4.16.0" && echo v4.16.0'
check "k3d"                   k3d version

echo "go modules"
for m in sigs.k8s.io/controller-runtime@v0.25.2 k8s.io/client-go@v0.37.1 \
	github.com/anishathalye/porcupine@v1.3.1 github.com/jackc/pgx/v5@v5.11.0; do
	check "$m" go list -m "$m"
done

echo "images"
for i in \
	postgres:18.6-bookworm \
	ghcr.io/chaos-mesh/chaos-mesh:v2.8.5 \
	ghcr.io/chaos-mesh/chaos-daemon:v2.8.5 \
	ghcr.io/cloudnative-pg/cloudnative-pg:1.30.1 \
	rancher/k3s:v1.36.5-k3s1 \
	rancher/k3s:v1.37.1-k3s1 \
	rancher/system-upgrade-controller:v0.20.2; do
	check "$i" crane digest "$i"
done

echo "postgres binaries"
check "pg_rewind/pg_basebackup/pg_ctl present" docker run --rm postgres:18.6-bookworm \
	sh -c 'cd /usr/lib/postgresql/18/bin && ls pg_rewind pg_basebackup pg_ctl pg_controldata | tr "\n" " "'

echo "kernel ($(uname -r))"
kconfig() {
	if [[ -r /proc/config.gz ]]; then zcat /proc/config.gz; else cat "/boot/config-$(uname -r)"; fi
}
for opt in CONFIG_IP_SET CONFIG_NETFILTER_XT_SET; do
	check "$opt (NetworkChaos partition)" bash -c "$(declare -f kconfig); kconfig | grep -E '^$opt=(y|m)'"
done
if kconfig | grep -qE '^CONFIG_NET_SCH_NETEM=(y|m)'; then
	echo "  ok    CONFIG_NET_SCH_NETEM (NetworkChaos delay/loss)"
else
	echo "  warn  CONFIG_NET_SCH_NETEM missing: delay/loss/corrupt NetworkChaos unavailable (partition unaffected)"
fi

exit $fail
