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
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgv1 "github.com/brv52/pg-operator/api/v1alpha1"
	"github.com/brv52/pg-operator/internal/pg"
)

// These specs drive Reconcile by hand against a real API server (envtest)
// and the fake SQL layer, so every round is deterministic: the test decides
// what each instance reports, advances the clock, and inspects the objects
// and SQL calls the round produced. There is no kubelet, so the test plays
// its part (marking pods running, removing deleted pods).

type harness struct {
	ns    string
	name  string
	fake  *fakePG
	r     *PostgresClusterReconciler
	clock time.Time
}

func newHarness() *harness {
	ns := &corev1.Namespace{GenerateName: "pgc-"}
	Expect(k8sClient.Create(ctx, ns)).To(Succeed())
	h := &harness{ns: ns.Name, name: "db", fake: newFakePG(), clock: time.Unix(1_800_000_000, 0)}
	h.r = &PostgresClusterReconciler{
		Client: k8sClient, Scheme: k8sClient.Scheme(), PG: h.fake,
		Now: func() time.Time { return h.clock },
	}
	return h
}

func (h *harness) key(name string) types.NamespacedName {
	return types.NamespacedName{Namespace: h.ns, Name: name}
}

func (h *harness) inst(n int) string { return fmt.Sprintf("%s-%d", h.name, n) }

func (h *harness) create(mutate ...func(*pgv1.PostgresCluster)) {
	c := &pgv1.PostgresCluster{
		Name: h.name, Namespace: h.ns,
		Spec: pgv1.PostgresClusterSpec{
			Instances:   3,
			Synchronous: &pgv1.SynchronousSpec{Method: pgv1.SyncMethodAny, Number: 1},
		},
	}
	for _, m := range mutate {
		m(c)
	}
	Expect(k8sClient.Create(ctx, c)).To(Succeed())
}

// tick advances the clock by one probe interval and runs one round.
func (h *harness) tick() {
	GinkgoHelper()
	h.clock = h.clock.Add(time.Second)
	_, err := h.r.Reconcile(ctx, reconcile.Request{NamespacedName: h.key(h.name)})
	Expect(err).NotTo(HaveOccurred())
}

func (h *harness) cluster() *pgv1.PostgresCluster {
	GinkgoHelper()
	var c pgv1.PostgresCluster
	Expect(k8sClient.Get(ctx, h.key(h.name), &c)).To(Succeed())
	return &c
}

func (h *harness) pod(name string) *corev1.Pod {
	var p corev1.Pod
	if err := k8sClient.Get(ctx, h.key(name), &p); err != nil {
		return nil
	}
	return &p
}

// run plays the kubelet: the pod gets an IP and becomes Ready.
func (h *harness) run(name string) {
	GinkgoHelper()
	p := h.pod(name)
	Expect(p).NotTo(BeNil(), "pod %s", name)
	ord := strings.TrimPrefix(name, h.name+"-")
	p.Status.Phase = corev1.PodRunning
	p.Status.PodIP = "10.0.0." + ord
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
}

// restart plays a container restart of postgres in the pod, as after a
// crash or the node coming back.
func (h *harness) restart(name string) {
	GinkgoHelper()
	p := h.pod(name)
	Expect(p).NotTo(BeNil())
	var n int32
	if len(p.Status.ContainerStatuses) > 0 {
		n = p.Status.ContainerStatuses[0].RestartCount + 1
	}
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: containerPostgres, Image: "postgres", ImageID: "postgres", Ready: true, RestartCount: n,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}}
	Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
}

func (h *harness) kill(name string) {
	GinkgoHelper()
	p := h.pod(name)
	Expect(p).NotTo(BeNil())
	Expect(k8sClient.Delete(ctx, p, client.GracePeriodSeconds(0))).To(Succeed())
	h.fake.down(name)
}

func (h *harness) env(p *corev1.Pod, key string) string {
	for _, e := range p.Spec.InitContainers[0].Env {
		if e.Name == key {
			return e.Value
		}
	}
	return ""
}

// healthy brings a three-instance cluster to Healthy with db-1 primary and
// both standbys streaming.
func (h *harness) healthy() {
	GinkgoHelper()
	h.create()
	h.tick()
	h.run(h.inst(1))
	h.fake.primary(h.inst(1), 100)
	h.tick()
	Expect(h.cluster().Status.CurrentPrimary).To(Equal(h.inst(1)))

	h.tick()
	for _, n := range []int{2, 3} {
		h.run(h.inst(n))
		h.fake.standby(h.inst(n), 100, true)
	}
	h.fake.primary(h.inst(1), 100, h.inst(2), h.inst(3))
	h.tick()
	h.tick()
	c := h.cluster()
	Expect(c.Status.FailoverPhase).To(Equal(pgv1.PhaseHealthy))
	Expect(c.Status.ReadyInstances).To(Equal(int32(3)))
}

var _ = Describe("PostgresCluster controller", func() {
	var h *harness
	BeforeEach(func() { h = newHarness() })

	It("bootstraps a primary, then clones replicas from it", func() {
		h.create()
		h.tick()

		p1 := h.pod(h.inst(1))
		Expect(p1).NotTo(BeNil())
		Expect(p1.Labels).To(HaveKeyWithValue(pgv1.LabelRole, pgv1.RolePrimary))
		Expect(h.env(p1, "ROLE")).To(Equal("primary"))
		Expect(h.env(p1, "ALLOW_INITDB")).To(Equal("true"))
		Expect(h.pod(h.inst(2))).To(BeNil(), "replicas wait for a working primary")

		for _, n := range []string{"db-rw", "db-ro", "db-instances"} {
			var svc corev1.Service
			Expect(k8sClient.Get(ctx, h.key(n), &svc)).To(Succeed())
		}
		var rw corev1.Service
		Expect(k8sClient.Get(ctx, h.key("db-rw"), &rw)).To(Succeed())
		Expect(rw.Spec.Selector).To(Equal(map[string]string{pgv1.LabelCluster: "db", pgv1.LabelRole: pgv1.RolePrimary}))
		var pdb policyv1.PodDisruptionBudget
		Expect(k8sClient.Get(ctx, h.key("db-primary"), &pdb)).To(Succeed())
		Expect(pdb.Spec.MinAvailable.IntValue()).To(Equal(1))
		Expect(k8sClient.Get(ctx, h.key("db-replicas"), &pdb)).To(Succeed())
		Expect(pdb.Spec.MaxUnavailable).To(BeNil())
		Expect(pdb.Spec.MinAvailable.IntValue()).To(Equal(1), "three instances: one of two standbys may be evicted")

		h.run(h.inst(1))
		h.fake.primary(h.inst(1), 100)
		h.tick()
		c := h.cluster()
		Expect(c.Status.CurrentPrimary).To(Equal(h.inst(1)))
		Expect(c.Status.Epoch).To(Equal(int64(1)))

		h.tick()
		for _, n := range []int{2, 3} {
			p := h.pod(h.inst(n))
			Expect(p).NotTo(BeNil())
			Expect(p.Labels).To(HaveKeyWithValue(pgv1.LabelRole, pgv1.RoleReplica))
			Expect(h.env(p, "ROLE")).To(Equal("replica"))
			Expect(h.env(p, "PRIMARY_HOST")).To(HavePrefix("db-1.db-instances."))
		}
		Expect(h.fake.get(h.inst(1)).slots).To(ConsistOf(pg.SlotName(h.inst(2)), pg.SlotName(h.inst(3))))
		Expect(h.fake.get(h.inst(1)).settings).To(HaveKeyWithValue("synchronous_standby_names", `ANY 1 ("db-2", "db-3")`))
	})

	It("stages the synchronous quorum on standbys before they are ever promoted", func() {
		h.healthy()
		s2 := h.fake.get(h.inst(2)).settings
		Expect(s2).To(HaveKeyWithValue("synchronous_standby_names", `ANY 1 ("db-1", "db-3")`))
		Expect(s2["primary_conninfo"]).To(ContainSubstring("host=db-1.db-instances."))
		Expect(s2["primary_conninfo"]).To(ContainSubstring("application_name=db-2"))
		Expect(s2).To(HaveKeyWithValue("primary_slot_name", "pgop_db_2"))
	})

	It("fails over to the standby with the highest received LSN when the primary pod is killed", func() {
		h.healthy()
		h.fake.standby(h.inst(2), 100, false)
		h.fake.standby(h.inst(3), 250, false)
		h.kill(h.inst(1))

		h.tick()

		c := h.cluster()
		Expect(c.Status.FailoverPhase).To(Equal(pgv1.PhaseHealthy))
		Expect(c.Status.CurrentPrimary).To(Equal(h.inst(3)))
		Expect(c.Status.Epoch).To(Equal(int64(2)))
		Expect(c.Status.LastFailoverDuration).NotTo(BeNil())
		Expect(h.fake.promotedNames()).To(Equal([]string{h.inst(3)}))
		Expect(h.fake.detached).To(ConsistOf(h.inst(2), h.inst(3)),
			"both standbys stop receiving WAL before LSNs are compared")

		p3 := h.pod(h.inst(3))
		Expect(p3.Labels).To(HaveKeyWithValue(pgv1.LabelRole, pgv1.RolePrimary))
		Expect(p3.Annotations).To(HaveKeyWithValue(pgv1.AnnotationEpoch, "2"))
		Expect(h.fake.get(h.inst(2)).settings["primary_conninfo"]).To(ContainSubstring("host=db-3.db-instances."))
		Expect(h.fake.get(h.inst(3)).slots).To(ConsistOf(pg.SlotName(h.inst(1)), pg.SlotName(h.inst(2))))

		var lease coordinationv1.Lease
		Expect(k8sClient.Get(ctx, h.key("db-primary"), &lease)).To(Succeed())
		Expect(*lease.Spec.HolderIdentity).To(Equal(h.inst(3)))
		Expect(*lease.Spec.LeaseTransitions).To(Equal(int32(2)))

		// The old primary comes back as a replica of the new one, which
		// makes its init container rewind against db-3.
		h.fake.primary(h.inst(3), 260, h.inst(2))
		h.tick()
		p1 := h.pod(h.inst(1))
		Expect(p1).NotTo(BeNil())
		Expect(p1.Labels).To(HaveKeyWithValue(pgv1.LabelRole, pgv1.RoleReplica))
		Expect(h.env(p1, "ROLE")).To(Equal("replica"))
		Expect(h.env(p1, "PRIMARY_HOST")).To(HavePrefix("db-3.db-instances."))
	})

	It("gives a re-pointed standby time to follow the new timeline before recreating it", func() {
		h.healthy()
		h.fake.standby(h.inst(2), 100, false)
		h.fake.standby(h.inst(3), 250, false)
		h.kill(h.inst(1))
		h.tick()
		Expect(h.cluster().Status.CurrentPrimary).To(Equal(h.inst(3)))
		uid := h.pod(h.inst(2)).UID

		// db-3 is primary on timeline 2; db-2 is still on timeline 1 and
		// reconnecting. That is normal for a second or two.
		h.fake.primary(h.inst(3), 260)
		for range 5 {
			h.tick()
		}
		Expect(h.pod(h.inst(2)).UID).To(Equal(uid), "a standby catching up must not be recreated")

		// Still stuck half a minute later: it cannot follow, rewind it.
		h.clock = h.clock.Add(divergedAfter)
		h.tick()
		Expect(h.pod(h.inst(2))).To(BeNil())
	})

	It("needs K consecutive failed probe rounds and resets on recovery", func() {
		h.healthy()
		h.fake.down(h.inst(1))

		h.tick()
		Expect(h.cluster().Status.FailoverPhase).To(Equal(pgv1.PhaseDetecting))
		Expect(h.cluster().Status.ProbeFailures).To(Equal(int32(1)))

		// A reconcile triggered by a watch event inside the same probe
		// interval does not count as another failure.
		_, err := h.r.Reconcile(ctx, reconcile.Request{NamespacedName: h.key(h.name)})
		Expect(err).NotTo(HaveOccurred())
		Expect(h.cluster().Status.ProbeFailures).To(Equal(int32(1)))

		h.tick()
		Expect(h.cluster().Status.ProbeFailures).To(Equal(int32(2)))

		h.fake.primary(h.inst(1), 120, h.inst(2), h.inst(3))
		h.tick()
		c := h.cluster()
		Expect(c.Status.FailoverPhase).To(Equal(pgv1.PhaseHealthy))
		Expect(c.Status.ProbeFailures).To(BeZero())
		Expect(h.fake.promotedNames()).To(BeEmpty())

		h.fake.down(h.inst(1))
		h.fake.standby(h.inst(2), 120, false)
		h.fake.standby(h.inst(3), 120, false)
		h.tick()
		h.tick()
		Expect(h.fake.promotedNames()).To(BeEmpty())
		h.tick()
		Expect(h.fake.promotedNames()).To(Equal([]string{h.inst(2)}), "tie on LSN goes to the lowest ordinal")
		Expect(h.pod(h.inst(1))).To(BeNil(), "fencing deletes the failed primary")
	})

	It("keeps a failed primary when no standby could replace it", func() {
		h.healthy()
		epoch := h.cluster().Status.Epoch
		h.fake.standby(h.inst(3), 300, false)
		h.fake.down(h.inst(2))
		h.kill(h.inst(1))

		h.tick()
		c := h.cluster()
		Expect(c.Status.FailoverPhase).To(Equal(pgv1.PhaseDetecting), "fencing needs an electable successor")
		Expect(c.Status.Epoch).To(Equal(epoch))
		Expect(meta.FindStatusCondition(c.Status.Conditions, pgv1.ConditionFailover).Reason).To(Equal("NoQuorum"))
		Expect(h.fake.fencedNames()).To(BeEmpty())
		p := h.pod(h.inst(1))
		Expect(p).NotTo(BeNil(), "the primary pod is recreated in place")
		Expect(p.Labels).To(HaveKeyWithValue(pgv1.LabelRole, pgv1.RolePrimary))
		Expect(h.env(p, "ROLE")).To(Equal(pgv1.RolePrimary))
		Expect(h.env(p, "ALLOW_INITDB")).To(BeEmpty(), "a recreated primary must find its data, never create it")

		h.run(h.inst(1))
		h.fake.primary(h.inst(1), 300, h.inst(3))
		h.tick()
		c = h.cluster()
		Expect(c.Status.FailoverPhase).To(Equal(pgv1.PhaseHealthy))
		Expect(c.Status.CurrentPrimary).To(Equal(h.inst(1)))
		Expect(h.fake.promotedNames()).To(BeEmpty())
	})

	It("waits for a quorum after fencing, then aborts and restores the old primary", func() {
		h.healthy()
		h.kill(h.inst(1))
		h.fake.standby(h.inst(3), 300, false)
		h.fake.down(h.inst(2))
		c := h.cluster()
		c.Status.FailoverPhase = pgv1.PhasePromoting
		c.Status.FailoverReason = pgv1.ReasonPodMissing
		c.Status.Epoch = 2
		c.Status.FailoverStartTime = &metav1.Time{Time: h.clock}
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())

		h.tick()
		c = h.cluster()
		Expect(c.Status.FailoverPhase).To(Equal(pgv1.PhasePromoting))
		Expect(meta.FindStatusCondition(c.Status.Conditions, pgv1.ConditionFailover).Reason).To(Equal("NoQuorum"))
		Expect(h.pod(h.inst(1))).To(BeNil(), "the old primary stays down while a successor may still appear")

		h.clock = h.clock.Add(abortFailoverAfter)
		h.tick()
		c = h.cluster()
		Expect(c.Status.FailoverPhase).To(Equal(pgv1.PhaseDetecting))
		Expect(c.Status.CurrentPrimary).To(Equal(h.inst(1)))
		Expect(c.Status.TargetPrimary).To(BeEmpty())
		Expect(h.fake.promotedNames()).To(BeEmpty())
		p := h.pod(h.inst(1))
		Expect(p).NotTo(BeNil())
		Expect(p.Labels).To(HaveKeyWithValue(pgv1.LabelRole, pgv1.RolePrimary))

		h.run(h.inst(1))
		h.fake.primary(h.inst(1), 300, h.inst(3))
		h.tick()
		c = h.cluster()
		Expect(c.Status.FailoverPhase).To(Equal(pgv1.PhaseHealthy))
		Expect(c.Status.CurrentPrimary).To(Equal(h.inst(1)))
		Expect(c.Status.Epoch).To(Equal(int64(2)))
	})

	It("resumes a failover from the persisted phase after an operator restart", func() {
		h.healthy()
		h.kill(h.inst(1))
		// db-3 is further ahead now, but the previous operator instance
		// already elected db-2 and may have run pg_promote on it.
		h.fake.standby(h.inst(2), 100, false)
		h.fake.standby(h.inst(3), 500, false)
		c := h.cluster()
		c.Status.FailoverPhase = pgv1.PhasePromoting
		c.Status.FailoverReason = pgv1.ReasonPodMissing
		c.Status.TargetPrimary = h.inst(2)
		c.Status.Epoch = 2
		c.Status.FailoverStartTime = &metav1.Time{Time: h.clock}
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())

		h.r = &PostgresClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), PG: h.fake,
			Now: func() time.Time { return h.clock }}
		h.tick()

		c = h.cluster()
		Expect(c.Status.CurrentPrimary).To(Equal(h.inst(2)))
		Expect(c.Status.Epoch).To(Equal(int64(2)), "resuming must not start a new epoch")
		Expect(h.fake.promotedNames()).To(Equal([]string{h.inst(2)}))
	})

	It("re-elects when the chosen target dies before it is promoted", func() {
		h.healthy()
		h.kill(h.inst(1))
		h.fake.standby(h.inst(3), 500, false)
		c := h.cluster()
		c.Status.FailoverPhase = pgv1.PhasePromoting
		c.Status.TargetPrimary = h.inst(2)
		c.Status.Epoch = 2
		c.Status.FailoverStartTime = &metav1.Time{Time: h.clock}
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
		h.kill(h.inst(2))

		h.tick()
		c = h.cluster()
		Expect(c.Status.TargetPrimary).To(BeEmpty())
		Expect(c.Status.FailoverPhase).To(Equal(pgv1.PhasePromoting), "one standby left is below quorum")
		Expect(h.fake.promotedNames()).To(BeEmpty())
	})

	It("detects a primary partitioned from its standbys even though the operator can reach it", func() {
		h.healthy()
		// The primary still answers the operator, but its walsenders are
		// gone and both standbys have lost their WAL stream.
		h.fake.primary(h.inst(1), 400)
		h.fake.standby(h.inst(2), 380, false)
		h.fake.standby(h.inst(3), 390, false)

		h.tick()
		Expect(h.cluster().Status.FailoverReason).To(Equal(pgv1.ReasonIsolatedFromReplicas))
		h.tick()
		h.tick()

		c := h.cluster()
		Expect(c.Status.CurrentPrimary).To(Equal(h.inst(3)))
		Expect(h.fake.fencedNames()).To(Equal([]string{h.inst(1)}), "the reachable old primary is SQL-fenced")
		Expect(h.pod(h.inst(1))).To(BeNil())
	})

	It("does not mistake a whole-cluster restart for a partition", func() {
		h.healthy()
		// Every pod restarted. The primary is back first and its standbys
		// have not reconnected yet, exactly what a partition looks like
		// to the primary.
		for n := 1; n <= 3; n++ {
			h.restart(h.inst(n))
		}
		h.fake.primary(h.inst(1), 400)
		h.fake.standby(h.inst(2), 380, false)
		h.fake.standby(h.inst(3), 390, false)
		for range 4 {
			h.tick()
		}
		c := h.cluster()
		Expect(c.Status.FailoverPhase).To(Equal(pgv1.PhaseHealthy))
		Expect(c.Status.CurrentPrimary).To(Equal(h.inst(1)))
		Expect(h.fake.fencedNames()).To(BeEmpty())

		// The same holds for an operator that restarted along with them:
		// it has seen nothing streaming yet.
		h.r = &PostgresClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), PG: h.fake,
			Now: func() time.Time { return h.clock }}
		for range 4 {
			h.tick()
		}
		Expect(h.cluster().Status.FailoverPhase).To(Equal(pgv1.PhaseHealthy))

		// Once the standbys have streamed again, losing them is evidence.
		h.fake.primary(h.inst(1), 400, h.inst(2), h.inst(3))
		h.fake.standby(h.inst(2), 400, true)
		h.fake.standby(h.inst(3), 400, true)
		h.tick()
		h.fake.primary(h.inst(1), 450)
		h.fake.standby(h.inst(2), 420, false)
		h.fake.standby(h.inst(3), 430, false)
		for range 3 {
			h.tick()
		}
		Expect(h.cluster().Status.CurrentPrimary).To(Equal(h.inst(3)))
	})

	It("re-elects when the promotion target stays unreachable", func() {
		h.healthy()
		h.kill(h.inst(1))
		h.fake.standby(h.inst(3), 400, false)
		h.fake.down(h.inst(2))
		c := h.cluster()
		c.Status.FailoverPhase = pgv1.PhasePromoting
		c.Status.FailoverReason = pgv1.ReasonPodMissing
		c.Status.TargetPrimary = h.inst(2)
		c.Status.Epoch = 2
		c.Status.FailoverStartTime = &metav1.Time{Time: h.clock}
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())

		h.tick()
		c = h.cluster()
		Expect(c.Status.TargetPrimary).To(Equal(h.inst(2)), "a briefly unreachable target is waited for")
		Expect(h.fake.promotedNames()).To(BeEmpty())

		h.clock = h.clock.Add(abortFailoverAfter)
		h.tick()
		c = h.cluster()
		Expect(h.pod(h.inst(2))).To(BeNil(), "the abandoned target is removed so it can only rejoin as a standby")
		Expect(c.Status.TargetPrimary).To(BeEmpty())
		Expect(h.fake.promotedNames()).To(BeEmpty())
		// db-3 alone is no quorum, so the failover is aborted and the old
		// primary comes back on its volume.
		Expect(c.Status.CurrentPrimary).To(Equal(h.inst(1)))
		p := h.pod(h.inst(1))
		Expect(p).NotTo(BeNil())
		Expect(p.Labels).To(HaveKeyWithValue(pgv1.LabelRole, pgv1.RolePrimary))
	})

	It("recreates a standby that cannot follow, first to rewind and then to reclone", func() {
		h.healthy()
		attempts := func() string {
			var pvc corev1.PersistentVolumeClaim
			Expect(k8sClient.Get(ctx, h.key(h.inst(2)), &pvc)).To(Succeed())
			return pvc.Annotations[pgv1.AnnotationRejoinAttempts]
		}
		// postgres runs in db-2 but never becomes reachable: recovery
		// stopped at WAL the primary does not have.
		stall := func() {
			GinkgoHelper()
			h.restart(h.inst(2))
			h.fake.down(h.inst(2))
			h.tick()
			h.clock = h.clock.Add(stuckAfter)
			h.tick()
		}

		stall()
		Expect(h.pod(h.inst(2))).To(BeNil())
		Expect(attempts()).To(Equal("1"))
		Expect(h.fake.promotedNames()).To(BeEmpty())
		h.tick()
		p := h.pod(h.inst(2))
		Expect(p).NotTo(BeNil())
		Expect(h.env(p, "RECLONE")).To(BeEmpty(), "the first attempt rejoins through pg_rewind")

		h.run(h.inst(2))
		stall()
		Expect(h.pod(h.inst(2))).To(BeNil())
		Expect(attempts()).To(Equal("2"))
		h.tick()
		Expect(h.env(h.pod(h.inst(2)), "RECLONE")).To(Equal("true"))

		h.run(h.inst(2))
		h.fake.standby(h.inst(2), 100, true)
		h.tick()
		Expect(attempts()).To(BeEmpty(), "streaming again resets the count")
	})

	It("does not mistake replicas that never streamed for a partition", func() {
		h.create()
		h.tick()
		h.run(h.inst(1))
		h.fake.primary(h.inst(1), 100)
		h.tick()
		h.tick()
		for _, n := range []int{2, 3} {
			h.run(h.inst(n))
			h.fake.standby(h.inst(n), 0, false)
		}
		for range 5 {
			h.tick()
		}
		Expect(h.cluster().Status.FailoverPhase).To(Equal(pgv1.PhaseHealthy))
		Expect(h.fake.promotedNames()).To(BeEmpty())
	})

	It("switches over when the primary's node is cordoned", func() {
		h.healthy()
		node := &corev1.Node{GenerateName: "node-"}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		p1 := h.pod(h.inst(1))
		Expect(k8sClient.SubResource("binding").Create(ctx, p1, &corev1.Binding{
			Name: p1.Name, Namespace: h.ns,
			Target: corev1.ObjectReference{Kind: "Node", Name: node.Name},
		})).To(Succeed())
		h.tick()
		Expect(h.cluster().Status.FailoverPhase).To(Equal(pgv1.PhaseHealthy))

		node.Spec.Unschedulable = true
		Expect(k8sClient.Update(ctx, node)).To(Succeed())
		h.fake.standby(h.inst(2), 100, true)
		h.fake.standby(h.inst(3), 100, true)
		h.tick()

		c := h.cluster()
		Expect(c.Status.FailoverReason).To(Equal(pgv1.ReasonSwitchover))
		Expect(c.Status.FailoverPhase).To(Equal(pgv1.PhasePromoting), "waits for the graceful shutdown")
		Expect(h.fake.fencedNames()).To(Equal([]string{h.inst(1)}))
		p1 = h.pod(h.inst(1))
		Expect(p1.DeletionTimestamp).NotTo(BeNil())
		Expect(p1.Labels).To(HaveKeyWithValue(pgv1.LabelRole, pgv1.RoleFenced))

		// Play the kubelet finishing the graceful shutdown.
		Expect(k8sClient.Delete(ctx, p1, client.GracePeriodSeconds(0))).To(Succeed())
		h.fake.down(h.inst(1))
		h.tick()
		c = h.cluster()
		Expect(c.Status.FailoverPhase).To(Equal(pgv1.PhaseHealthy))
		Expect(c.Status.CurrentPrimary).To(Equal(h.inst(2)))
	})

	It("never fails over a single-instance cluster", func() {
		h.create(func(c *pgv1.PostgresCluster) { c.Spec.Instances = 1; c.Spec.Synchronous = nil })
		h.tick()
		h.run(h.inst(1))
		h.fake.primary(h.inst(1), 100)
		h.tick()
		h.kill(h.inst(1))
		h.tick()
		Expect(h.cluster().Status.FailoverPhase).To(Equal(pgv1.PhaseHealthy))
		p := h.pod(h.inst(1))
		Expect(p).NotTo(BeNil(), "recreated in place")
		Expect(h.env(p, "ROLE")).To(Equal("primary"))
	})
})
