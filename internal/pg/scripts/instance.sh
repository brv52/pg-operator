#!/bin/sh
# Instance lifecycle inside a pg-operator pod.
#
#   instance.sh init   init container: create, clone or rewind PGDATA
#   instance.sh run    main container: start postgres in the right role
#
# The role an instance boots in is the pod's current role label, read from
# the downward API, falling back to the ROLE env the pod was created with.
# The label matters after a node restart: the init container runs again,
# and a standby that has since been promoted must not be treated as the
# replica it was created as. A former primary is always recreated with
# role=replica, so it can only come back through pg_rewind or a fresh clone
# and never as a second primary.
#
# Environment (set by the operator):
#   POD_NAME            instance name, also the replication application_name
#   ROLE                primary | replica
#   ALLOW_INITDB        "true" only on the pod that bootstraps a new cluster
#   RW_HOST             the cluster's -rw Service, preferred clone/rewind source
#   PRIMARY_HOST        the primary when the pod was created, fallback source
#   PGDATA              data directory on the volume
#   SUPERUSER_PASSWORD  from the cluster Secret
#   REPLICATION_PASSWORD
#   SLOT_NAME           physical slot reserved for this instance on the primary
#   STATIC_FLAGS        postgres -c flags rendered by the operator
set -eu

VOLUME=$(dirname "$PGDATA")
PASSFILE="$VOLUME/.pgpass"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) instance[$POD_NAME] $*"; }

write_passfile() {
	umask 077
	{
		echo "*:*:*:replicator:$REPLICATION_PASSWORD"
		echo "*:*:*:postgres:$SUPERUSER_PASSWORD"
	} >"$PASSFILE"
	export PGPASSFILE="$PASSFILE"
}

# set_auto KEY VALUE rewrites one setting in postgresql.auto.conf. It is
# only used while postgres is down; at runtime the operator uses ALTER SYSTEM.
set_auto() {
	f="$PGDATA/postgresql.auto.conf"
	touch "$f"
	grep -v "^$1 = " "$f" >"$f.tmp" || true
	printf "%s = '%s'\n" "$1" "$(printf %s "$2" | sed "s/'/''/g")" >>"$f.tmp"
	mv "$f.tmp" "$f"
}

current_role() {
	label_role=$(sed -n 's/^pg.platform.local\/role="\(.*\)"$/\1/p' /etc/podinfo/labels 2>/dev/null || true)
	echo "${label_role:-$ROLE}"
}

# find_primary prints a host that answers as a writable primary, trying the
# -rw Service first (it follows failovers) and the creation-time primary
# second. It gives up after the given number of seconds.
find_primary() {
	deadline=$(($(date +%s) + $1))
	while :; do
		for h in ${RW_HOST:-} "$PRIMARY_HOST"; do
			if [ "$(psql "host=$h port=5432 user=postgres dbname=postgres connect_timeout=2" -XAtc \
				'SELECT NOT pg_is_in_recovery()' 2>/dev/null)" = t ]; then
				echo "$h"
				return 0
			fi
		done
		[ "$(date +%s)" -ge "$deadline" ] && return 1
		sleep 2
	done
}

conninfo() {
	echo "host=$1 port=5432 user=replicator application_name=$POD_NAME passfile=$PASSFILE sslmode=prefer connect_timeout=2 keepalives=1 keepalives_idle=2 keepalives_interval=1 keepalives_count=3 tcp_user_timeout=5000"
}

configure_standby() {
	set_auto primary_conninfo "$(conninfo "$1")"
	set_auto primary_slot_name "$SLOT_NAME"
	# A fenced primary may carry read_only from the operator's SQL fence.
	set_auto default_transaction_read_only off
	touch "$PGDATA/standby.signal"
}

bootstrap_primary() {
	log "initdb"
	pwfile=$(mktemp)
	printf %s "$SUPERUSER_PASSWORD" >"$pwfile"
	initdb -D "$PGDATA" --username=postgres --pwfile="$pwfile" \
		--auth-local=trust --auth-host=scram-sha-256 --data-checksums --encoding=UTF8 --locale=C.UTF-8
	rm -f "$pwfile"

	log "creating replication role"
	pg_ctl -D "$PGDATA" -w -o "-c listen_addresses='' -c unix_socket_directories=/tmp" start
	psql -v ON_ERROR_STOP=1 -h /tmp -U postgres -d postgres \
		-v pw="$REPLICATION_PASSWORD" <<-'SQL'
		CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD :'pw';
	SQL
	pg_ctl -D "$PGDATA" -w -m fast stop
}

clone() {
	log "cloning from $1"
	rm -rf "$PGDATA.new"
	# Clone into a side directory and rename at the end so that a pod
	# killed mid-copy never leaves a half-written PGDATA that the next
	# start would mistake for a valid one.
	pg_basebackup -D "$PGDATA.new" -d "$(conninfo "$1")" \
		--wal-method=stream --checkpoint=fast --no-password --progress --verbose
	rm -rf "$PGDATA"
	mv "$PGDATA.new" "$PGDATA"
	chmod 700 "$PGDATA"
}

rewind() {
	log "rewinding against $1"
	rm -f "$PGDATA/standby.signal"
	# After a kill, postmaster.pid still names the old postmaster, usually
	# PID 1. Inside this init container PID 1 is alive (it is us), so
	# postgres would refuse to start for crash recovery. Nothing else can
	# be running on this RWO volume while the init container runs.
	rm -f "$PGDATA/postmaster.pid"
	# pg_rewind runs crash recovery itself when the target was not shut
	# down cleanly (the usual case after a pod kill). When the timelines
	# have not diverged it reports "no rewind required" and exits 0.
	pg_rewind --target-pgdata="$PGDATA" \
		--source-server="host=$1 port=5432 user=postgres dbname=postgres passfile=$PASSFILE connect_timeout=5" \
		--progress
}

cmd_init() {
	mkdir -p "$PGDATA"
	chmod 700 "$PGDATA"
	write_passfile
	rm -rf "$PGDATA.new"
	role=$(current_role)

	if [ "$role" = primary ]; then
		if [ -s "$PGDATA/PG_VERSION" ]; then
			log "existing primary data directory, nothing to do"
		elif [ "${ALLOW_INITDB:-}" = true ]; then
			bootstrap_primary
		else
			# A primary recreated on a volume whose data is gone. An
			# empty database here would silently replace the real one
			# and the standbys would be recloned from it.
			log "role is primary but PGDATA is empty, refusing to initdb"
			find "$VOLUME" -maxdepth 1 -name "$(basename "$PGDATA").failed.*" -printf '  set aside earlier: %p\n' || true
			exit 1
		fi
		return
	fi

	if [ ! -s "$PGDATA/PG_VERSION" ]; then
		# Nothing to lose: wait as long as it takes for a primary.
		until source=$(find_primary 60); do
			log "no writable primary reachable yet, waiting to clone"
		done
		clone "$source"
		configure_standby "$source"
		return
	fi

	if ! source=$(find_primary 30); then
		# Typically a whole-cluster restart: the primary is down or still
		# being elected. This data directory may hold the most recent WAL
		# in the cluster, so it must not be touched. Start as a standby;
		# the operator can elect it, and if it turns out to have diverged
		# it is recreated (and rewound) once a primary exists.
		log "no writable primary reachable, starting as a standby without rewinding"
		configure_standby "$PRIMARY_HOST"
		return
	fi

	if ! rewind "$source"; then
		# The source is a reachable primary and rewind still failed: the
		# WAL needed to rewind is gone (recycled beyond wal_keep_size and
		# slot retention) or the data directory is damaged. A full clone
		# always works. Keep the newest failed copy for forensics only.
		log "pg_rewind against a reachable primary failed, falling back to a full clone"
		find "$VOLUME" -maxdepth 1 -name "$(basename "$PGDATA").failed.*" -exec rm -rf {} + || true
		mv "$PGDATA" "$PGDATA.failed.$(date +%s)"
		clone "$source"
	fi
	configure_standby "$source"
}

cmd_run() {
	write_passfile
	# Self-fence on container restart. The init container does not run
	# again when postgres crashes and the kubelet restarts this container,
	# so the role is re-read here. An instance the operator no longer
	# considers primary must not boot without standby.signal.
	role=$(current_role)
	if [ "$role" != primary ] && [ ! -f "$PGDATA/standby.signal" ]; then
		log "role is $role, refusing to start as primary"
		touch "$PGDATA/standby.signal"
	fi
	if [ -f /etc/pg/custom.conf ] && ! grep -q "^include_if_exists = '/etc/pg/custom.conf'" "$PGDATA/postgresql.conf"; then
		echo "include_if_exists = '/etc/pg/custom.conf'" >>"$PGDATA/postgresql.conf"
	fi
	# STATIC_FLAGS is split on spaces but must not be globbed
	# (listen_addresses=*).
	set -f
	# shellcheck disable=SC2086
	exec postgres -D "$PGDATA" $STATIC_FLAGS
}

case "${1:-}" in
init) cmd_init ;;
run) cmd_run ;;
*)
	echo "usage: $0 init|run" >&2
	exit 2
	;;
esac
