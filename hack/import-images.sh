#!/bin/sh
# Import local images into the k3d nodes, one image and one node at a time.
#
#   hack/import-images.sh CLUSTER IMAGE...
#
# An image whose tag is already present on every node is skipped unless its
# tag ends in ":dev" (those are rebuilt in place). The pinned PostgreSQL
# image is the largest one and never changes, so re-importing it on every
# deploy only costs time and page cache, and WSL2 does not hand page cache
# back to Windows by default.
set -eu

cluster=$1
shift
nodes=$(k3d node list -o json | jq -r --arg c "$cluster" \
	'.[] | select(.runtimeLabels["k3d.cluster"] == $c and (.role == "server" or .role == "agent")) | .name')

on_all_nodes() {
	for n in $nodes; do
		docker exec "$n" crictl inspecti "$1" >/dev/null 2>&1 || return 1
	done
}

for image in "$@"; do
	case $image in
	*/*) ref=$image ;;
	*) ref=docker.io/library/$image ;;
	esac
	case $image in
	*:dev) ;;
	*)
		if on_all_nodes "$ref"; then
			echo "skip $image (already on all nodes)"
			continue
		fi
		;;
	esac
	docker image inspect "$image" >/dev/null 2>&1 || docker pull -q "$image"
	# Stream straight into each node's containerd: no tarball on disk and
	# no k3d tools container. A single platform: with Docker's containerd
	# image store a plain save references layers of platforms that were
	# never pulled, and the import fails with "content digest not found".
	for n in $nodes; do
		echo "import $image into $n"
		docker save --platform linux/amd64 "$image" |
			docker exec -i "$n" ctr -n k8s.io images import --all-platforms=false - >/dev/null
	done
done
