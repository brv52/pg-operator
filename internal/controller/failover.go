/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgv1 "github.com/brv52/pg-operator/api/v1alpha1"
	"github.com/brv52/pg-operator/internal/pg"
)

// switchoverShutdownTimeout bounds how long a switchover waits for the old
// primary pod to stop before electing anyway. A graceful fast shutdown
// takes a few seconds; the pod's grace period is 30s.
const switchoverShutdownTimeout = 45 * time.Second

// errWait means the current phase cannot make progress yet. It is not a
// failure; the next round retries.
var errWait = errors.New("waiting")

// step advances the failover state machine as far as it can in this round.
// Every transition is persisted before the next phase's side effects run.
//
//	Healthy ──K failed rounds──▶ Fencing ──▶ Promoting ──▶ Reconfiguring ──▶ Healthy
//	   │  ▲                                                                       │
//	   └──┴── Detecting (0 < failures < K) ◀──────────────────────────────────────┘
func (rc *round) step(ctx context.Context) error {
	c := rc.c
	if c.Status.FailoverPhase == "" {
		c.Status.FailoverPhase = pgv1.PhaseHealthy
	}
	for range 4 {
		var err error
		before := c.Status.FailoverPhase
		switch before {
		case pgv1.PhaseHealthy, pgv1.PhaseDetecting:
			err = rc.detect(ctx)
		case pgv1.PhaseFencing:
			err = rc.fence(ctx)
		case pgv1.PhasePromoting:
			err = rc.promote(ctx)
		case pgv1.PhaseReconfiguring:
			err = rc.reconfigure(ctx)
		}
		if errors.Is(err, errWait) {
			return nil
		}
		if err != nil {
			return err
		}
		if c.Status.FailoverPhase == before || c.Status.FailoverPhase == pgv1.PhaseHealthy {
			return nil
		}
	}
	return nil
}

// primaryFailure decides whether this round counts as a failed observation
// of the primary. immediate is true when the evidence is definitive (the
// pod is gone or being deleted), in which case waiting for K rounds only
// adds downtime.
func (rc *round) primaryFailure() (failed, immediate bool, reason string) {
	c := rc.c
	p := rc.set.get(c.Status.CurrentPrimary)
	switch {
	case p == nil || p.pod == nil:
		return true, true, pgv1.ReasonPodMissing
	case p.terminating():
		return true, true, pgv1.ReasonPodTerminating
	case !p.obs.Reachable:
		return true, false, pgv1.ReasonProbeFailed
	case p.obs.InRecovery:
		return true, false, pgv1.ReasonPrimaryInRecovery
	}
	if rc.isolatedFromStandbys(p) {
		return true, false, pgv1.ReasonIsolatedFromReplicas
	}
	return false, false, ""
}

// isolatedFromStandbys is the standby-side half of failure detection. The
// operator's own SQL probe only proves the operator can reach the primary;
// a network partition that cuts the primary off from its standbys and its
// clients but not from the operator would otherwise go unnoticed while
// every write blocks on synchronous replication.
//
// The primary counts as isolated when, at the same time:
//   - the primary itself sees no streaming standby, and
//   - enough standbys to form an election quorum are reachable, and every
//     one of them reports its WAL receiver down or silent for longer than
//     wal_receiver_timeout, and
//   - each of them was streaming in the previous recorded status, so a
//     standby that is still being cloned or has never connected does not
//     look like evidence against the primary.
func (rc *round) isolatedFromStandbys(p *instance) bool {
	c := rc.c
	if len(p.obs.StreamingReplicas(pg.WalSenderTimeout)) > 0 {
		return false
	}
	prev := map[string]pgv1.InstanceStatus{}
	for _, s := range c.Status.Instances {
		prev[s.Name] = s
	}
	inDetection := c.Status.FailoverPhase == pgv1.PhaseDetecting &&
		c.Status.FailoverReason == pgv1.ReasonIsolatedFromReplicas

	standbys := rc.set.others(p.name)
	var witnesses int
	for _, s := range standbys {
		if !s.obs.Reachable || !s.obs.InRecovery {
			continue
		}
		if s.healthyStandby() {
			return false
		}
		if !inDetection && !prev[s.name].Streaming {
			continue
		}
		witnesses++
	}
	return witnesses > 0 && witnesses >= pg.RequiredVoters(len(standbys), rc.syncNumber())
}

func (rc *round) syncNumber() int {
	if s := rc.c.Spec.Synchronous; s != nil {
		return int(s.Number)
	}
	return 0
}

// detect implements Healthy and Detecting.
func (rc *round) detect(ctx context.Context) error {
	c := rc.c
	failed, immediate, reason := rc.primaryFailure()

	if !failed {
		if c.Status.FailoverPhase != pgv1.PhaseHealthy || c.Status.ProbeFailures != 0 {
			rc.log.Info("primary recovered before failover threshold", "failures", c.Status.ProbeFailures)
			c.Status.FailoverPhase = pgv1.PhaseHealthy
			c.Status.ProbeFailures = 0
			c.Status.FailoverReason = ""
		}
		if sw := rc.switchoverNeeded(); sw != "" {
			rc.log.Info("starting switchover", "reason", sw)
			rc.r.event(c, corev1.EventTypeNormal, pgv1.ReasonSwitchover, "%s", sw)
			return rc.enterFencing(ctx, pgv1.ReasonSwitchover)
		}
		return nil
	}

	if c.Spec.Instances == 1 {
		// Nothing to fail over to; converge recreates the pod in place.
		return nil
	}

	// Enter Detecting even when this round is not counted, so evidence
	// that depends on the previous round (see isolatedFromStandbys) is
	// carried forward.
	c.Status.FailoverPhase = pgv1.PhaseDetecting
	c.Status.FailoverReason = reason
	if !rc.r.countRound(c, immediate) {
		return nil
	}
	k := c.Spec.Failover.FailureThresholdK
	c.Status.ProbeFailures++
	if immediate {
		c.Status.ProbeFailures = k
	}
	if c.Status.ProbeFailures < k {
		rc.log.Info("primary probe failed", "reason", reason, "failures", c.Status.ProbeFailures, "threshold", k)
		return nil
	}
	rc.r.event(c, corev1.EventTypeWarning, "PrimaryFailed", "primary %s failed: %s", c.Status.CurrentPrimary, reason)
	return rc.enterFencing(ctx, reason)
}

// countRound reports whether a failed probe round should count towards K.
// Rounds closer together than 80% of the probe interval are coalesced.
func (r *PostgresClusterReconciler) countRound(c *pgv1.PostgresCluster, immediate bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastCounted == nil {
		r.lastCounted = map[types.NamespacedName]time.Time{}
	}
	key := client.ObjectKeyFromObject(c)
	now := r.now()
	if last, ok := r.lastCounted[key]; ok && !immediate &&
		now.Sub(last) < c.Spec.Failover.ProbeInterval.Duration*8/10 {
		return false
	}
	r.lastCounted[key] = now
	return true
}

func (rc *round) enterFencing(ctx context.Context, reason string) error {
	c := rc.c
	now := metav1.NewTime(rc.r.now())
	c.Status.FailoverPhase = pgv1.PhaseFencing
	c.Status.FailoverReason = reason
	c.Status.FailoverStartTime = &now
	c.Status.TargetPrimary = ""
	c.Status.Epoch++
	meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{
		Type: pgv1.ConditionFailover, Status: metav1.ConditionTrue, Reason: reason,
		Message: fmt.Sprintf("replacing primary %s (epoch %d)", c.Status.CurrentPrimary, c.Status.Epoch),
	})
	failoversTotal.WithLabelValues(c.Namespace, c.Name, reason).Inc()
	return rc.persist(ctx)
}

// fence takes the old primary out of service. All three steps are
// idempotent so the phase can be retried after a crash at any point.
func (rc *round) fence(ctx context.Context) error {
	c := rc.c
	old := rc.set.get(c.Status.CurrentPrimary)
	switchover := c.Status.FailoverReason == pgv1.ReasonSwitchover

	// 2a. Stop routing: the fenced label matches neither Service.
	if old != nil && old.pod != nil && old.role() != pgv1.RoleFenced {
		if err := rc.setRole(ctx, old, pgv1.RoleFenced); err != nil {
			return err
		}
	}
	// Best effort SQL fence for a primary we can still reach (partition
	// from its standbys, or a planned switchover): new transactions are
	// read-only and existing sessions are dropped.
	if old != nil && old.obs.Reachable && !old.obs.InRecovery {
		fctx, cancel := context.WithTimeout(ctx, c.Spec.Failover.ProbeTimeout.Duration)
		if err := rc.r.PG.Fence(fctx, old.target(rc.password)); err != nil {
			rc.log.Info("SQL fence failed, relying on synchronous replication", "error", err.Error())
		}
		cancel()
		if switchover {
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			_ = rc.r.PG.Checkpoint(cctx, old.target(rc.password))
			cancel()
		}
	}

	// 2b. Publish the new epoch with no holder.
	if err := rc.writeLease(ctx, "", true); err != nil {
		return err
	}

	// 2c. Remove the old primary pod. On a Ready node with a failed
	// primary, grace 0 makes the kubelet kill it immediately. A switchover
	// uses the normal grace period so the fast shutdown ships the final
	// WAL to the standbys. On an unreachable node a grace-0 delete would
	// only remove the API object while the container keeps running, so
	// the normal delete is used and the pod is not recreated until it is
	// really gone (its RWO volume keeps a replacement off other nodes).
	if old != nil && old.pod != nil && !old.terminating() {
		opts := []client.DeleteOption{client.Preconditions{UID: &old.pod.UID}}
		if !switchover && rc.nodeReady(ctx, old.pod.Spec.NodeName) {
			opts = append(opts, client.GracePeriodSeconds(0))
		}
		if err := rc.r.Delete(ctx, old.pod, opts...); client.IgnoreNotFound(err) != nil && !apierrors.IsConflict(err) {
			return err
		}
	}
	rc.r.event(c, corev1.EventTypeNormal, "Fenced", "fenced %s in epoch %d", c.Status.CurrentPrimary, c.Status.Epoch)
	c.Status.FailoverPhase = pgv1.PhasePromoting
	return rc.persist(ctx)
}

func (rc *round) nodeReady(ctx context.Context, name string) bool {
	if name == "" {
		return false
	}
	var n corev1.Node
	if err := rc.r.Get(ctx, client.ObjectKey{Name: name}, &n); err != nil {
		return false
	}
	for _, cond := range n.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// promote elects and promotes the successor. The elected name is persisted
// before pg_promote runs: after the promotion the LSNs change, and a
// re-election by a restarted operator could otherwise pick a different
// standby and produce two primaries.
func (rc *round) promote(ctx context.Context) error {
	c := rc.c
	oldName := c.Status.CurrentPrimary

	if c.Status.TargetPrimary != "" {
		t := rc.set.get(c.Status.TargetPrimary)
		if t == nil || t.pod == nil || t.terminating() {
			// The chosen standby died before it was promoted. It never
			// left recovery (Promoting would have moved on), so electing
			// again among the rest is safe.
			rc.log.Info("promotion target lost, re-electing", "target", c.Status.TargetPrimary)
			c.Status.TargetPrimary = ""
			if err := rc.persist(ctx); err != nil {
				return err
			}
		}
	}

	if c.Status.TargetPrimary == "" {
		if c.Status.FailoverReason == pgv1.ReasonSwitchover {
			if old := rc.set.get(oldName); old != nil && old.pod != nil &&
				rc.r.now().Sub(c.Status.FailoverStartTime.Time) < switchoverShutdownTimeout {
				return errWait
			}
		}
		var cands []pg.Candidate
		standbys := rc.set.others(oldName)
		for _, s := range standbys {
			cands = append(cands, pg.Candidate{
				Name: s.name, Ordinal: s.ordinal, Reachable: s.obs.Reachable && !s.terminating(),
				InRecovery: s.obs.InRecovery, Timeline: s.obs.Timeline,
				ReceiveLSN: s.obs.ReceiveLSN, ReplayLSN: s.obs.ReplayLSN,
			})
		}
		winner, err := pg.Elect(cands, len(standbys), rc.syncNumber())
		if err != nil {
			meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{
				Type: pgv1.ConditionFailover, Status: metav1.ConditionTrue, Reason: "NoQuorum", Message: err.Error(),
			})
			rc.log.Info("cannot elect a new primary yet", "error", err.Error())
			return errWait
		}
		rc.log.Info("elected new primary", "instance", winner.Name, "receiveLSN", winner.ReceiveLSN.String(), "timeline", winner.Timeline)
		c.Status.TargetPrimary = winner.Name
		if err := rc.persist(ctx); err != nil {
			return err
		}
	}

	target := rc.set.get(c.Status.TargetPrimary)
	if err := rc.writeLease(ctx, target.name, true); err != nil {
		return err
	}
	pctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	if err := rc.r.PG.Promote(pctx, target.target(rc.password), 30*time.Second); err != nil {
		return fmt.Errorf("promoting %s: %w", target.name, err)
	}
	if err := rc.setRole(ctx, target, pgv1.RolePrimary); err != nil {
		return err
	}
	rc.r.event(c, corev1.EventTypeNormal, "Promoted", "promoted %s in epoch %d", target.name, c.Status.Epoch)
	c.Status.CurrentPrimary = target.name
	c.Status.FailoverPhase = pgv1.PhaseReconfiguring
	return rc.persist(ctx)
}

// reconfigure points the surviving standbys at the new primary and
// finishes the failover.
func (rc *round) reconfigure(ctx context.Context) error {
	c := rc.c
	p := rc.set.get(c.Status.CurrentPrimary)
	pt := p.target(rc.password)
	timeout := 10 * time.Second

	others := rc.set.others(p.name)
	slots := make([]string, 0, len(others))
	for _, o := range others {
		slots = append(slots, pg.SlotName(o.name))
	}
	// Slots first, so re-pointed standbys do not bounce off a missing slot
	// and wait out wal_retrieve_retry_interval.
	sctx, cancel := context.WithTimeout(ctx, timeout)
	err := rc.r.PG.EnsureSlots(sctx, pt, slots)
	cancel()
	if err != nil {
		return fmt.Errorf("slots on %s: %w", p.name, err)
	}

	for _, s := range rc.set.others(p.name) {
		if !s.obs.Reachable || !s.obs.InRecovery || s.role() == pgv1.RoleFenced {
			continue
		}
		actx, cancel := context.WithTimeout(ctx, timeout)
		_, err := rc.r.PG.ApplySettings(actx, s.target(rc.password), rc.standbySettings(s.name, p.name))
		cancel()
		if err != nil {
			return fmt.Errorf("re-pointing %s: %w", s.name, err)
		}
	}

	// A checkpoint writes the new timeline into the control file, which
	// pg_rewind reads when the old primary rejoins.
	cctx, cancel := context.WithTimeout(ctx, timeout)
	if err := rc.r.PG.Checkpoint(cctx, pt); err != nil {
		rc.log.Info("post-promotion checkpoint failed", "error", err.Error())
	}
	cancel()

	start := c.Status.FailoverStartTime
	if start != nil {
		d := rc.r.now().Sub(start.Time)
		c.Status.LastFailoverDuration = &metav1.Duration{Duration: d}
		failoverSeconds.WithLabelValues(c.Namespace, c.Name, c.Status.FailoverReason).Observe(d.Seconds())
	}
	rc.r.event(c, corev1.EventTypeNormal, "FailoverCompleted", "%s is primary in epoch %d", p.name, c.Status.Epoch)
	c.Status.FailoverPhase = pgv1.PhaseHealthy
	c.Status.ProbeFailures = 0
	c.Status.FailoverReason = ""
	c.Status.FailoverStartTime = nil
	meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{
		Type: pgv1.ConditionFailover, Status: metav1.ConditionFalse, Reason: "Completed",
		Message: fmt.Sprintf("%s is primary in epoch %d", p.name, c.Status.Epoch),
	})
	return rc.persist(ctx)
}

// standbySettings are the dynamic settings of a standby named self
// following primary.
func (rc *round) standbySettings(self, primary string) map[string]string {
	s := rc.instanceSettings(self)
	s["primary_conninfo"] = pg.PrimaryConninfo(instanceHost(rc.c, primary), self)
	s["primary_slot_name"] = pg.SlotName(self)
	return s
}

// instanceSettings are the settings every instance carries regardless of
// role. synchronous_standby_names is staged on standbys too (where it is
// ignored) so that a promoted standby enforces the quorum from its very
// first commit, with no window of asynchronous acknowledgements.
func (rc *round) instanceSettings(self string) map[string]string {
	var others []string
	for _, i := range rc.set.all {
		if i.name != self && i.ordinal <= int(rc.c.Spec.Instances) {
			others = append(others, i.name)
		}
	}
	method := string(pgv1.SyncMethodAny)
	if s := rc.c.Spec.Synchronous; s != nil {
		method = string(s.Method)
	}
	return map[string]string{
		"synchronous_standby_names":     pg.SyncStandbyNames(method, rc.syncNumber(), others),
		"default_transaction_read_only": "off",
	}
}

func (rc *round) setRole(ctx context.Context, i *instance, role string) error {
	patch := client.MergeFrom(i.pod.DeepCopy())
	i.pod.Labels[pgv1.LabelRole] = role
	if role == pgv1.RolePrimary {
		if i.pod.Annotations == nil {
			i.pod.Annotations = map[string]string{}
		}
		i.pod.Annotations[pgv1.AnnotationEpoch] = fmt.Sprint(rc.c.Status.Epoch)
	}
	if err := rc.r.Patch(ctx, i.pod, patch); err != nil {
		return fmt.Errorf("labelling %s %s: %w", i.name, role, err)
	}
	return nil
}
