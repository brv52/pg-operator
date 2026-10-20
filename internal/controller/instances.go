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
	"slices"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"

	pgv1 "github.com/brv52/pg-operator/api/v1alpha1"
	"github.com/brv52/pg-operator/internal/pg"
)

// instance is everything the reconciler knows about one ordinal in this
// round: its objects and, if the pod could be probed, its SQL state.
type instance struct {
	name    string
	ordinal int
	pod     *corev1.Pod
	pvc     *corev1.PersistentVolumeClaim
	obs     pg.Observation
}

func (i *instance) role() string {
	if i.pod == nil {
		return ""
	}
	return i.pod.Labels[pgv1.LabelRole]
}

// postgresRunning reports whether the postgres container has started, as
// opposed to the pod still running its init container.
func (i *instance) postgresRunning() bool {
	if i.pod == nil {
		return false
	}
	for _, cs := range i.pod.Status.ContainerStatuses {
		if cs.Name == containerPostgres {
			return cs.State.Running != nil
		}
	}
	return false
}

// rejoinAttempts is the number of consecutive recreations recorded on the
// instance's PVC.
func (i *instance) rejoinAttempts() int {
	if i.pvc == nil {
		return 0
	}
	n, _ := strconv.Atoi(i.pvc.Annotations[pgv1.AnnotationRejoinAttempts])
	return n
}

func (i *instance) terminating() bool {
	return i.pod != nil && i.pod.DeletionTimestamp != nil
}

// running is true when the pod exists, is not being deleted and has an
// IP to probe.
func (i *instance) running() bool {
	return i.pod != nil && !i.terminating() && i.pod.Status.PodIP != "" &&
		i.pod.Status.Phase == corev1.PodRunning
}

func (i *instance) podReady() bool {
	if !i.running() {
		return false
	}
	for _, c := range i.pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// healthyStandby reports whether the instance is a reachable standby whose
// WAL receiver is connected and recently heard from its upstream.
// incarnation identifies the running postgres process well enough to tell
// whether it has restarted: a new pod or a container restart (a crash, or
// the node coming back) changes it.
func (i *instance) incarnation() string {
	if i.pod == nil {
		return ""
	}
	for _, cs := range i.pod.Status.ContainerStatuses {
		if cs.Name == containerPostgres {
			return fmt.Sprintf("%s/%d", i.pod.UID, cs.RestartCount)
		}
	}
	return string(i.pod.UID)
}

func (i *instance) healthyStandby() bool {
	return i.obs.Reachable && i.obs.InRecovery && i.obs.Streaming &&
		(i.obs.LastMsgAge < 0 || i.obs.LastMsgAge <= pg.WalReceiverTimeout)
}

type instanceSet struct {
	byName map[string]*instance
	// ordered by ordinal, includes ordinals above spec.instances that
	// still have objects (scale-down leftovers).
	all []*instance
}

func (s *instanceSet) get(name string) *instance {
	if s == nil {
		return nil
	}
	return s.byName[name]
}

// others returns every instance except the named one, in ordinal order.
func (s *instanceSet) others(name string) []*instance {
	var out []*instance
	for _, i := range s.all {
		if i.name != name {
			out = append(out, i)
		}
	}
	return out
}

// buildInstanceSet indexes pods and PVCs by ordinal. Ordinals 1..desired are
// always present (possibly with no objects yet).
func buildInstanceSet(c *pgv1.PostgresCluster, pods []corev1.Pod, pvcs []corev1.PersistentVolumeClaim) *instanceSet {
	s := &instanceSet{byName: map[string]*instance{}}
	add := func(name string) *instance {
		if i, ok := s.byName[name]; ok {
			return i
		}
		ord, ok := c.InstanceOrdinal(name)
		if !ok {
			return nil
		}
		i := &instance{name: name, ordinal: ord, obs: pg.Observation{Name: name}}
		s.byName[name] = i
		s.all = append(s.all, i)
		return i
	}
	for n := 1; n <= int(c.Spec.Instances); n++ {
		add(c.InstanceName(n))
	}
	for k := range pods {
		if i := add(pods[k].Name); i != nil {
			i.pod = &pods[k]
		}
	}
	for k := range pvcs {
		if i := add(pvcs[k].Name); i != nil {
			i.pvc = &pvcs[k]
		}
	}
	slices.SortFunc(s.all, func(a, b *instance) int { return a.ordinal - b.ordinal })
	return s
}

// observeAll probes every running instance in parallel, each bounded by
// timeout, so one hung instance cannot stretch a probe round.
func observeAll(ctx context.Context, client pg.Client, s *instanceSet, password string, timeout time.Duration) {
	var wg sync.WaitGroup
	for _, i := range s.all {
		if !i.running() {
			i.obs = pg.Observation{Name: i.name}
			continue
		}
		wg.Go(func() {
			pctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			i.obs = client.Observe(pctx, i.target(password))
		})
	}
	wg.Wait()
}

func (i *instance) target(password string) pg.Target {
	t := pg.Target{Name: i.name, Password: password}
	if i.pod != nil {
		t.Host = i.pod.Status.PodIP
	}
	return t
}
