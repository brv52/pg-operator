#!/bin/sh
# Instance lifecycle inside a pg-operator pod.
#
#   instance.sh init   init container: create, clone or rewind PGDATA
#   instance.sh run    main container: start postgres in the right role
#
# The role an instance boots in is decided by the operator when it creates
# the pod (ROLE env, from the role label at creation time) and is never
# changed for the lifetime of the pod. A former primary is always recreated
# with ROLE=replica, so it can only come back through pg_rewind or a fresh
# clone and never as a second primary.
#
# Environment (set by the operator):
#   POD_NAME            instance name, also the replication application_name
#   ROLE                primary | replica
#   PRIMARY_HOST        clone/rewind source (replica only)
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

conninfo() {
	echo "host=$1 port=5432 user=replicator application_name=$POD_NAME passfile=$PASSFILE sslmode=prefer connect_timeout=2 keepalives=1 keepalives_idle=2 keepalives_interval=1 keepalives_count=3 tcp_user_timeout=5000"
}

configure_standby() {
	set_auto primary_conninfo "$(conninfo "$PRIMARY_HOST")"
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
	log "cloning from $PRIMARY_HOST"
	rm -rf "$PGDATA.new"
	# Clone into a side directory and rename at the end so that a pod
	# killed mid-copy never leaves a half-written PGDATA that the next
	# start would mistake for a valid one.
	pg_basebackup -D "$PGDATA.new" -d "$(conninfo "$PRIMARY_HOST")" \
		--wal-method=stream --checkpoint=fast --no-password --progress --verbose
	rm -rf "$PGDATA"
	mv "$PGDATA.new" "$PGDATA"
	chmod 700 "$PGDATA"
}

rewind() {
	log "rewinding against $PRIMARY_HOST"
	rm -f "$PGDATA/standby.signal"
	# pg_rewind runs crash recovery itself when the target was not shut
	# down cleanly (the usual case after a pod kill). When the timelines
	# have not diverged it reports "no rewind required" and exits 0.
	pg_rewind --target-pgdata="$PGDATA" \
		--source-server="host=$PRIMARY_HOST port=5432 user=postgres dbname=postgres passfile=$PASSFILE connect_timeout=5" \
		--progress
}

cmd_init() {
	mkdir -p "$PGDATA"
	chmod 700 "$PGDATA"
	write_passfile
	rm -rf "$PGDATA.new"

	if [ "$ROLE" = primary ]; then
		if [ -s "$PGDATA/PG_VERSION" ]; then
			log "existing primary data directory, nothing to do"
		else
			bootstrap_primary
		fi
		return
	fi

	if [ -s "$PGDATA/PG_VERSION" ]; then
		if ! rewind; then
			# The WAL needed to rewind is gone (recycled on the new
			# primary and beyond wal_keep_size/slot retention), or the
			# data directory is damaged. A full clone always works.
			log "pg_rewind failed, falling back to a full clone"
			mv "$PGDATA" "$PGDATA.failed.$(date +%s)"
			find "$VOLUME" -maxdepth 1 -name "$(basename "$PGDATA").failed.*" -mmin +60 -exec rm -rf {} + || true
			clone
		fi
	else
		clone
	fi
	configure_standby
}

cmd_run() {
	write_passfile
	# Self-fence on container restart. The init container only runs when
	# the pod is created; if postgres crashes and the kubelet restarts this
	# container, the pod's current role comes from the downward API. An
	# instance the operator no longer considers primary must not boot
	# without standby.signal.
	label_role=$(sed -n 's/^pg.platform.local\/role="\(.*\)"$/\1/p' /etc/podinfo/labels 2>/dev/null || true)
	if [ "${label_role:-$ROLE}" != primary ] && [ ! -f "$PGDATA/standby.signal" ]; then
		log "role is ${label_role:-$ROLE}, refusing to start as primary"
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
