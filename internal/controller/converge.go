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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgv1 "github.com/brv52/pg-operator/api/v1alpha1"
	"github.com/brv52/pg-operator/internal/pg"
)

// convergeTimeout bounds each SQL call made while converging.
const convergeTimeout = 5 * time.Second

// converge runs only in the Healthy phase. It brings labels, pods,
// replication slots and dynamic settings in line with the current primary
// and the spec. Every action is idempotent and safe to repeat each round.
func (rc *round) converge(ctx context.Context) error {
	c := rc.c
	p := rc.set.get(c.Status.CurrentPrimary)

	if c.Spec.Instances == 1 && p.pod == nil {
		if err := rc.ensurePVC(ctx, p); err != nil {
			return err
		}
		return client.IgnoreAlreadyExists(rc.r.Create(ctx, buildPod(c, p.name, pgv1.RolePrimary, p.name, c.Status.Epoch)))
	}

	if err := rc.convergeLabels(ctx, p); err != nil {
		return err
	}
	if !p.obs.Reachable || p.obs.InRecovery {
		// Without a working primary there is nothing to clone from or
		// configure; detection handles the rest.
		return nil
	}

	if err := rc.convergeSQL(ctx, p); err != nil {
		return err
	}
	if err := rc.recreateDivergedStandbys(ctx, p); err != nil {
		return err
	}
	if err := rc.convergePods(ctx, p); err != nil {
		return err
	}
	return rc.rollReplicas(ctx)
}

func (rc *round) convergeLabels(ctx context.Context, p *instance) error {
	if p.pod != nil && !p.terminating() && p.role() != pgv1.RolePrimary && p.obs.Reachable && !p.obs.InRecovery {
		if err := rc.setRole(ctx, p, pgv1.RolePrimary); err != nil {
			return err
		}
	}
	for _, i := range rc.set.others(p.name) {
		if i.pod == nil || i.terminating() {
			continue
		}
		switch i.role() {
		case pgv1.RoleReplica:
		case pgv1.RoleFenced, pgv1.RolePrimary:
			// A leftover of an earlier epoch: an old primary whose
			// delete did not go through. It must come back through the
			// bootstrap script as a replica.
			rc.log.Info("deleting stale primary pod", "pod", i.name, "role", i.role())
			if err := rc.r.Delete(ctx, i.pod, client.Preconditions{UID: &i.pod.UID}); client.IgnoreNotFound(err) != nil {
				return err
			}
		default:
			if err := rc.setRole(ctx, i, pgv1.RoleReplica); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rc *round) convergeSQL(ctx context.Context, p *instance) error {
	var slots []string
	for _, o := range rc.set.others(p.name) {
		if o.ordinal <= int(rc.c.Spec.Instances) {
			slots = append(slots, pg.SlotName(o.name))
		}
	}
	sctx, cancel := context.WithTimeout(ctx, convergeTimeout)
	err := rc.r.PG.EnsureSlots(sctx, p.target(rc.password), slots)
	cancel()
	if err != nil {
		return fmt.Errorf("slots: %w", err)
	}

	apply := func(i *instance, settings map[string]string) {
		actx, cancel := context.WithTimeout(ctx, convergeTimeout)
		defer cancel()
		changed, err := rc.r.PG.ApplySettings(actx, i.target(rc.password), settings)
		if err != nil {
			rc.log.Error(err, "applying settings", "instance", i.name)
			return
		}
		if len(changed) > 0 {
			rc.log.Info("settings changed", "instance", i.name, "settings", changed)
		}
	}
	apply(p, rc.instanceSettings(p.name))
	for _, s := range rc.set.others(p.name) {
		if s.obs.Reachable && s.obs.InRecovery && s.role() == pgv1.RoleReplica {
			apply(s, rc.standbySettings(s.name, p.name))
		}
	}
	return nil
}

// divergedAfter is how long a standby must stay off the primary's timeline
// without streaming before it is recreated. Right after a failover every
// surviving standby is briefly in exactly that state while it reconnects
// and follows the timeline switch, which takes a second or two.
const divergedAfter = 30 * time.Second

// recreateDivergedStandbys deletes standbys that have been on an older
// timeline than the primary, without streaming, for longer than
// divergedAfter. That is the signature of an instance that cannot follow
// the timeline switch, typically a former primary whose container
// restarted in place (the self-fence kept it out of service but it still
// carries diverged WAL). The recreated pod goes through pg_rewind.
func (rc *round) recreateDivergedStandbys(ctx context.Context, p *instance) error {
	now := rc.r.now()
	for _, s := range rc.set.others(p.name) {
		key := client.ObjectKeyFromObject(rc.c).String() + "/" + s.name
		lagging := s.obs.Reachable && s.obs.InRecovery && !s.obs.Streaming && !s.terminating() &&
			s.obs.Timeline < p.obs.Timeline
		since, seen := rc.r.offTimelineSince(key, lagging, now)
		if !lagging || !seen || now.Sub(since) < divergedAfter {
			continue
		}
		rc.log.Info("standby cannot follow the primary timeline, recreating it", "instance", s.name,
			"timeline", s.obs.Timeline, "primaryTimeline", p.obs.Timeline, "since", since)
		rc.r.event(rc.c, corev1.EventTypeWarning, "StandbyDiverged", "recreating %s to rewind it", s.name)
		if err := rc.r.Delete(ctx, s.pod, client.Preconditions{UID: &s.pod.UID}); client.IgnoreNotFound(err) != nil {
			return err
		}
		rc.r.offTimelineSince(key, false, now)
	}
	return nil
}

// offTimelineSince tracks, in memory, when a standby was first seen lagging
// on an old timeline. It returns that time and whether it was already
// tracked before this call; passing lagging=false forgets the standby.
func (r *PostgresClusterReconciler) offTimelineSince(key string, lagging bool, now time.Time) (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !lagging {
		delete(r.offTimeline, key)
		return time.Time{}, false
	}
	if r.offTimeline == nil {
		r.offTimeline = map[string]time.Time{}
	}
	since, ok := r.offTimeline[key]
	if !ok {
		r.offTimeline[key] = now
		return now, false
	}
	return since, true
}

// convergePods creates missing instances as replicas of the current primary
// and removes instances above spec.instances.
func (rc *round) convergePods(ctx context.Context, p *instance) error {
	c := rc.c
	for _, i := range rc.set.all {
		if i.name == p.name {
			continue
		}
		if i.ordinal > int(c.Spec.Instances) {
			if err := rc.scaleDown(ctx, i); err != nil {
				return err
			}
			continue
		}
		if i.pod != nil {
			continue
		}
		if err := rc.ensurePVC(ctx, i); err != nil {
			return err
		}
		rc.log.Info("creating replica", "pod", i.name, "upstream", p.name)
		pod := buildPod(c, i.name, pgv1.RoleReplica, p.name, c.Status.Epoch)
		if err := rc.r.Create(ctx, pod); client.IgnoreAlreadyExists(err) != nil {
			return err
		}
	}
	return nil
}

func (rc *round) scaleDown(ctx context.Context, i *instance) error {
	if i.pod != nil && !i.terminating() {
		rc.log.Info("scaling down", "pod", i.name)
		if err := rc.r.Delete(ctx, i.pod); client.IgnoreNotFound(err) != nil {
			return err
		}
		return nil
	}
	if i.pod == nil && i.pvc != nil {
		return client.IgnoreNotFound(rc.r.Delete(ctx, i.pvc))
	}
	return nil
}

// rollReplicas recreates one out-of-date replica at a time, and only when
// every instance is ready, so a rolling update never removes more than one
// standby from the synchronous quorum. The primary is replaced last, by a
// switchover (see switchoverNeeded).
func (rc *round) rollReplicas(ctx context.Context) error {
	want := podSpecHash(rc.c)
	for _, i := range rc.set.all {
		if !i.podReady() || !i.obs.Reachable {
			return nil
		}
	}
	for _, i := range rc.set.others(rc.c.Status.CurrentPrimary) {
		if i.pod.Annotations[annotationSpecHash] == want {
			continue
		}
		rc.log.Info("rolling replica", "pod", i.name)
		return client.IgnoreNotFound(rc.r.Delete(ctx, i.pod, client.Preconditions{UID: &i.pod.UID}))
	}
	return nil
}

// switchoverNeeded returns a non-empty reason when the healthy primary
// should be replaced in a planned way: its node has been cordoned (a drain
// is coming, and the primary PDB is holding it back) or it is the last
// out-of-date pod of a rolling update. A switchover only starts when enough
// standbys are streaming to elect a successor right away.
func (rc *round) switchoverNeeded() string {
	c := rc.c
	if c.Spec.Instances < 2 {
		return ""
	}
	p := rc.set.get(c.Status.CurrentPrimary)
	if p == nil || p.pod == nil {
		return ""
	}
	standbys := rc.set.others(p.name)
	healthy := 0
	for _, s := range standbys {
		if s.healthyStandby() && s.podReady() {
			healthy++
		}
	}
	if healthy == 0 || healthy < pg.RequiredVoters(len(standbys), rc.syncNumber()) {
		return ""
	}

	var node corev1.Node
	if p.pod.Spec.NodeName != "" &&
		rc.r.Get(context.Background(), client.ObjectKey{Name: p.pod.Spec.NodeName}, &node) == nil &&
		node.Spec.Unschedulable {
		return fmt.Sprintf("node %s of primary %s is cordoned", node.Name, p.name)
	}

	want := podSpecHash(c)
	if p.pod.Annotations[annotationSpecHash] != want {
		for _, s := range standbys {
			if s.pod == nil || s.pod.Annotations[annotationSpecHash] != want {
				return ""
			}
		}
		return fmt.Sprintf("primary %s is the last instance with an outdated pod spec", p.name)
	}
	return ""
}
