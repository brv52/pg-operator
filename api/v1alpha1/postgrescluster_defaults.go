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

package v1alpha1

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Default fills every optional field with its documented default. The
// mutating webhook calls it, and the controller also calls it on a copy of
// the spec, so the operator behaves the same when webhooks are disabled or
// when an object predates a newly added default.
func (c *PostgresCluster) Default() {
	s := &c.Spec
	if s.Instances == 0 {
		s.Instances = 3
	}
	if s.PostgresImage == "" {
		s.PostgresImage = DefaultPostgresImage
	}
	if s.Synchronous != nil {
		if s.Synchronous.Method == "" {
			s.Synchronous.Method = SyncMethodAny
		}
		if s.Synchronous.Number == 0 {
			s.Synchronous.Number = 1
		}
	}
	if s.Storage.Size.IsZero() {
		s.Storage.Size = resource.MustParse("1Gi")
	}
	f := &s.Failover
	if f.ProbeInterval.Duration == 0 {
		f.ProbeInterval = metav1.Duration{Duration: time.Second}
	}
	if f.ProbeTimeout.Duration == 0 {
		f.ProbeTimeout = metav1.Duration{Duration: time.Second}
	}
	if f.FailureThresholdK == 0 {
		f.FailureThresholdK = 3
	}
	if f.LeaseDuration.Duration == 0 {
		f.LeaseDuration = metav1.Duration{Duration: 15 * time.Second}
	}
}

// InstanceName returns the pod and PVC name of the instance with the given
// 1-based ordinal.
func (c *PostgresCluster) InstanceName(ordinal int) string {
	return fmt.Sprintf("%s-%d", c.Name, ordinal)
}

// InstanceOrdinal parses the ordinal out of an instance name, returning
// false for names that do not belong to this cluster.
func (c *PostgresCluster) InstanceOrdinal(name string) (int, bool) {
	rest, ok := strings.CutPrefix(name, c.Name+"-")
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(rest)
	if err != nil || n < 1 {
		return 0, false
	}
	return n, true
}

// ReadWriteServiceName routes to the current primary only.
func (c *PostgresCluster) ReadWriteServiceName() string { return c.Name + "-rw" }

// ReadOnlyServiceName routes to streaming standbys only.
func (c *PostgresCluster) ReadOnlyServiceName() string { return c.Name + "-ro" }

// HeadlessServiceName gives every instance a stable DNS name used for
// replication connections that must not follow the -rw Service.
func (c *PostgresCluster) HeadlessServiceName() string { return c.Name + "-instances" }

// LeaseName is the coordination Lease that records primary and epoch.
func (c *PostgresCluster) LeaseName() string { return c.Name + "-primary" }

// SecretName holds the superuser and replication credentials.
func (c *PostgresCluster) SecretName() string { return c.Name + "-credentials" }

// ConfigMapName holds the instance bootstrap script.
func (c *PostgresCluster) ConfigMapName() string { return c.Name + "-scripts" }

// SelectorLabels are the labels every object of this cluster carries.
func (c *PostgresCluster) SelectorLabels() map[string]string {
	return map[string]string{LabelCluster: c.Name}
}
