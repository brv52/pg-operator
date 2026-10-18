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
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgv1 "github.com/brv52/pg-operator/api/v1alpha1"
	"github.com/brv52/pg-operator/internal/pg"
)

const podNodeIndex = "spec.nodeName"

// PostgresClusterReconciler reconciles a PostgresCluster object.
type PostgresClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// APIReader reads the PostgresCluster straight from the API server.
	// The failover phase is the state machine's source of truth; acting
	// on a cached copy that lags the previous round's status write would
	// either redo a phase or fail the next transition with a conflict and
	// lose a whole probe interval. Falls back to Client when nil.
	APIReader client.Reader
	PG        pg.Client
	Recorder  events.EventRecorder
	// Now is the clock; tests replace it.
	Now func() time.Time

	// lastCounted remembers when a failed probe round was last counted
	// per cluster. Reconciles are also triggered by watch events, and
	// without this a burst of pod updates during an outage would count
	// as several of the K consecutive failures. It is deliberately not
	// persisted: after an operator restart, detection simply starts over.
	mu          sync.Mutex
	lastCounted map[types.NamespacedName]time.Time
	// offTimeline records when each standby was first seen stuck on an
	// older timeline (see recreateDivergedStandbys).
	offTimeline map[string]time.Time
}

// +kubebuilder:rbac:groups=pg.platform.local,resources=postgresclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pg.platform.local,resources=postgresclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pg.platform.local,resources=postgresclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods;persistentvolumeclaims;services;secrets;configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile runs one round: make sure all supporting objects exist, probe
// every instance, advance the failover state machine as far as it can go
// without waiting, and, when healthy, converge replication settings and
// instance pods. It always requeues after the probe interval, because
// failure detection is an active process and not only a reaction to
// watch events.
func (r *PostgresClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cluster pgv1.PostgresCluster
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if err := reader.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if cluster.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}
	// Work on a defaulted copy so the operator behaves the same with or
	// without the mutating webhook.
	c := cluster.DeepCopy()
	c.Default()
	requeue := ctrl.Result{RequeueAfter: c.Spec.Failover.ProbeInterval.Duration}

	password, err := r.ensureSupportObjects(ctx, c)
	if err != nil {
		return ctrl.Result{}, err
	}

	set, err := r.loadInstances(ctx, c)
	if err != nil {
		return ctrl.Result{}, err
	}
	observeAll(ctx, r.PG, set, password, c.Spec.Failover.ProbeTimeout.Duration)

	rc := &round{r: r, c: c, set: set, password: password, log: log}

	if c.Status.CurrentPrimary == "" {
		if err := rc.bootstrap(ctx); err != nil {
			return ctrl.Result{}, err
		}
		return finish(requeue, rc.writeStatus(ctx))
	}

	if err := rc.step(ctx); err != nil {
		log.Error(err, "failover step failed", "phase", c.Status.FailoverPhase)
		// Keep the persisted phase; the next round retries it.
		_ = rc.writeStatus(ctx)
		return requeue, nil
	}
	if c.Status.FailoverPhase == pgv1.PhaseHealthy {
		if err := rc.converge(ctx); err != nil {
			log.Error(err, "convergence failed")
		}
	}
	if err := rc.ensureLease(ctx); err != nil {
		log.Error(err, "lease update failed")
	}
	return finish(requeue, rc.writeStatus(ctx))
}

// finish turns the final status write into a result. A conflict means
// someone (usually a user editing the spec) changed the object during the
// round; everything the round did is idempotent, so it is simply re-run
// right away against fresh state instead of being reported as an error.
func finish(requeue ctrl.Result, err error) (ctrl.Result, error) {
	switch {
	case err == nil:
		return requeue, nil
	case apierrors.IsConflict(err):
		return ctrl.Result{RequeueAfter: 50 * time.Millisecond}, nil
	default:
		return ctrl.Result{}, err
	}
}

// round carries the state of one reconcile.
type round struct {
	r        *PostgresClusterReconciler
	c        *pgv1.PostgresCluster
	set      *instanceSet
	password string
	log      interface {
		Info(string, ...any)
		Error(error, string, ...any)
	}
}

func (r *PostgresClusterReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *PostgresClusterReconciler) event(c *pgv1.PostgresCluster, typ, reason, note string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(c, nil, typ, reason, reason, note, args...)
	}
}

// ensureSupportObjects creates (and, where it is safe, updates) the Secret,
// ConfigMap, Services and PodDisruptionBudgets. It returns the superuser
// password the probes authenticate with.
func (r *PostgresClusterReconciler) ensureSupportObjects(ctx context.Context, c *pgv1.PostgresCluster) (string, error) {
	var secret corev1.Secret
	err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: c.SecretName()}, &secret)
	if apierrors.IsNotFound(err) {
		s, err := buildSecret(c)
		if err != nil {
			return "", err
		}
		if err := r.Create(ctx, s); err != nil {
			return "", err
		}
		secret = *s
		secret.Data = map[string][]byte{secretSuperuserKey: []byte(s.StringData[secretSuperuserKey])}
	} else if err != nil {
		return "", err
	}

	cm := buildConfigMap(c)
	if err := r.apply(ctx, cm, func(existing client.Object) {
		existing.(*corev1.ConfigMap).Data = cm.Data
	}); err != nil {
		return "", err
	}
	for _, svc := range buildServices(c) {
		if err := r.apply(ctx, svc, func(existing client.Object) {
			e := existing.(*corev1.Service)
			e.Spec.Selector = svc.Spec.Selector
			e.Spec.Ports = svc.Spec.Ports
			e.Spec.PublishNotReadyAddresses = svc.Spec.PublishNotReadyAddresses
		}); err != nil {
			return "", err
		}
	}
	for _, pdb := range buildPDBs(c) {
		if err := r.apply(ctx, pdb, func(existing client.Object) {
			existing.(*policyv1.PodDisruptionBudget).Spec = pdb.Spec
		}); err != nil {
			return "", err
		}
	}
	return string(secret.Data[secretSuperuserKey]), nil
}

// apply creates obj, or runs mutate on the existing object and updates it.
func (r *PostgresClusterReconciler) apply(ctx context.Context, obj client.Object, mutate func(client.Object)) error {
	existing := obj.DeepCopyObject().(client.Object)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, existing, func() error {
		if ts := existing.GetCreationTimestamp(); ts.IsZero() {
			return nil
		}
		mutate(existing)
		return nil
	})
	return err
}

func (r *PostgresClusterReconciler) loadInstances(ctx context.Context, c *pgv1.PostgresCluster) (*instanceSet, error) {
	sel := client.MatchingLabels(c.SelectorLabels())
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(c.Namespace), sel); err != nil {
		return nil, err
	}
	var pvcs corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcs, client.InNamespace(c.Namespace), sel); err != nil {
		return nil, err
	}
	return buildInstanceSet(c, pods.Items, pvcs.Items), nil
}

// writeStatus persists the observed instance state. Phase transitions are
// persisted earlier, by persist, before their side effects run.
func (rc *round) writeStatus(ctx context.Context) error {
	c := rc.c
	c.Status.Instances = c.Status.Instances[:0]
	var ready int32
	for _, i := range rc.set.all {
		st := pgv1.InstanceStatus{
			Name:      i.name,
			Role:      i.role(),
			Ready:     i.podReady() && i.obs.Reachable,
			Timeline:  i.obs.Timeline,
			Streaming: i.obs.Streaming,
		}
		if i.obs.Reachable {
			st.ReceiveLSN = i.obs.ReceiveLSN.String()
			st.ReplayLSN = i.obs.ReplayLSN.String()
		}
		if st.Ready {
			ready++
		}
		c.Status.Instances = append(c.Status.Instances, st)
	}
	c.Status.ReadyInstances = ready
	c.Status.ObservedGeneration = c.Generation

	readyCond := metav1.Condition{Type: pgv1.ConditionReady, Status: metav1.ConditionFalse, Reason: "Degraded",
		Message: fmt.Sprintf("%d/%d instances ready", ready, c.Spec.Instances)}
	switch {
	case c.Status.FailoverPhase != pgv1.PhaseHealthy:
		readyCond.Reason = "FailoverInProgress"
		c.Status.Phase = "Failover"
	case ready == c.Spec.Instances:
		readyCond.Status, readyCond.Reason = metav1.ConditionTrue, "AllInstancesReady"
		c.Status.Phase = "Healthy"
	default:
		c.Status.Phase = "Degraded"
	}
	meta.SetStatusCondition(&c.Status.Conditions, readyCond)
	return rc.updateStatus(ctx)
}

// updateStatus writes the status and re-applies defaults to the object the
// API server returns. Objects written by Go clients can carry explicit
// zero durations ("0s") that the CRD schema does not default, and the
// round must keep working with the defaulted values after every write.
func (rc *round) updateStatus(ctx context.Context) error {
	err := rc.r.Status().Update(ctx, rc.c)
	rc.c.Default()
	return err
}

// persist writes the status immediately. It is called on every state
// machine transition, before the transition's side effects, so a restarted
// operator resumes from the recorded phase. The update carries the
// resourceVersion read at the start of the round; if anything else wrote
// the object meanwhile, it fails and the round is retried from fresh state.
func (rc *round) persist(ctx context.Context) error {
	if err := rc.updateStatus(ctx); err != nil {
		return fmt.Errorf("persisting phase %s: %w", rc.c.Status.FailoverPhase, err)
	}
	return nil
}

// bootstrap creates the first instance as primary and records it once it
// answers as a primary.
func (rc *round) bootstrap(ctx context.Context) error {
	c := rc.c
	first := rc.set.get(c.InstanceName(1))
	if first.pod == nil {
		if err := rc.ensurePVC(ctx, first); err != nil {
			return err
		}
		pod := buildPod(c, first.name, pgv1.RolePrimary, first.name, 1)
		// Only this pod may create a database. Every later primary pod is
		// recreated on a volume that must already hold one, and an empty
		// one there means lost data, not a new cluster.
		bootstrap := &pod.Spec.InitContainers[0]
		bootstrap.Env = append(bootstrap.Env, corev1.EnvVar{Name: "ALLOW_INITDB", Value: "true"})
		rc.log.Info("bootstrapping first instance", "pod", first.name)
		return client.IgnoreAlreadyExists(rc.r.Create(ctx, pod))
	}
	if first.obs.Reachable && !first.obs.InRecovery {
		c.Status.CurrentPrimary = first.name
		c.Status.TargetPrimary = first.name
		c.Status.Epoch = 1
		c.Status.FailoverPhase = pgv1.PhaseHealthy
		rc.r.event(c, corev1.EventTypeNormal, "Bootstrapped", "instance %s is the primary", first.name)
	}
	return nil
}

func (rc *round) ensurePVC(ctx context.Context, i *instance) error {
	if i.pvc != nil {
		return nil
	}
	return client.IgnoreAlreadyExists(rc.r.Create(ctx, buildPVC(rc.c, i.name)))
}

// ensureLease mirrors the primary identity and epoch into a coordination
// Lease. The Lease is the cluster-visible record of who is primary in
// which epoch; leaseTransitions carries the epoch. It is renewed at a third
// of its duration rather than every round.
func (rc *round) ensureLease(ctx context.Context) error {
	return rc.writeLease(ctx, rc.c.Status.CurrentPrimary, false)
}

func (rc *round) writeLease(ctx context.Context, holder string, force bool) error {
	c := rc.c
	now := metav1.NewMicroTime(rc.r.now())
	dur := int32(c.Spec.Failover.LeaseDuration.Seconds())
	epoch := int32(c.Status.Epoch)

	var lease coordinationv1.Lease
	err := rc.r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: c.LeaseName()}, &lease)
	if apierrors.IsNotFound(err) {
		lease = coordinationv1.Lease{
			ObjectMeta: objectMeta(c, c.LeaseName(), nil),
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity: &holder, LeaseDurationSeconds: &dur,
				AcquireTime: &now, RenewTime: &now, LeaseTransitions: &epoch,
			},
		}
		return rc.r.Create(ctx, &lease)
	}
	if err != nil {
		return err
	}
	same := lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity == holder &&
		lease.Spec.LeaseTransitions != nil && *lease.Spec.LeaseTransitions == epoch
	fresh := lease.Spec.RenewTime != nil && rc.r.now().Sub(lease.Spec.RenewTime.Time) < c.Spec.Failover.LeaseDuration.Duration/3
	if same && fresh && !force {
		return nil
	}
	if !same {
		lease.Spec.AcquireTime = &now
	}
	lease.Spec.HolderIdentity = &holder
	lease.Spec.LeaseDurationSeconds = &dur
	lease.Spec.RenewTime = &now
	lease.Spec.LeaseTransitions = &epoch
	// Update (not patch) so a concurrent writer with a stale view fails on
	// resourceVersion instead of silently overwriting the epoch.
	return rc.r.Update(ctx, &lease)
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgresClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, podNodeIndex,
		func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} }); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgv1.PostgresCluster{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.Service{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		// A cordon on the primary's node triggers a switchover, so node
		// spec changes must reach the clusters that have pods there.
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(r.clustersOnNode),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(e event.UpdateEvent) bool {
					return e.ObjectOld.(*corev1.Node).Spec.Unschedulable != e.ObjectNew.(*corev1.Node).Spec.Unschedulable
				},
				CreateFunc:  func(event.CreateEvent) bool { return false },
				DeleteFunc:  func(event.DeleteEvent) bool { return true },
				GenericFunc: func(event.GenericEvent) bool { return false },
			})).
		WithOptions(controller.Options{MaxConcurrentReconciles: 4}).
		Named("postgrescluster").
		Complete(r)
}

func (r *PostgresClusterReconciler) clustersOnNode(ctx context.Context, o client.Object) []reconcile.Request {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.MatchingFields{podNodeIndex: o.GetName()},
		client.HasLabels{pgv1.LabelCluster}); err != nil {
		return nil
	}
	seen := map[types.NamespacedName]bool{}
	var out []reconcile.Request
	for _, p := range pods.Items {
		k := types.NamespacedName{Namespace: p.Namespace, Name: p.Labels[pgv1.LabelCluster]}
		if !seen[k] {
			seen[k] = true
			out = append(out, reconcile.Request{NamespacedName: k})
		}
	}
	return out
}
