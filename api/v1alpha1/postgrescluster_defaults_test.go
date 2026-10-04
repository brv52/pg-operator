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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDefault(t *testing.T) {
	c := &PostgresCluster{Spec: PostgresClusterSpec{Synchronous: &SynchronousSpec{}}}
	c.Default()

	if c.Spec.Instances != 3 {
		t.Errorf("instances = %d, want 3", c.Spec.Instances)
	}
	if c.Spec.PostgresImage != DefaultPostgresImage {
		t.Errorf("image = %q", c.Spec.PostgresImage)
	}
	if got := *c.Spec.Synchronous; got.Method != SyncMethodAny || got.Number != 1 {
		t.Errorf("synchronous = %+v, want any/1", got)
	}
	if c.Spec.Storage.Size.String() != "1Gi" {
		t.Errorf("storage = %s", c.Spec.Storage.Size.String())
	}
	f := c.Spec.Failover
	if f.ProbeInterval.Duration != time.Second || f.FailureThresholdK != 3 || f.LeaseDuration.Duration != 15*time.Second {
		t.Errorf("failover = %+v", f)
	}
}

func TestDefaultKeepsExplicitValues(t *testing.T) {
	c := &PostgresCluster{Spec: PostgresClusterSpec{
		Instances: 5,
		Failover:  FailoverSpec{FailureThresholdK: 7, ProbeInterval: metav1.Duration{Duration: 250 * time.Millisecond}},
	}}
	c.Default()
	if c.Spec.Instances != 5 || c.Spec.Failover.FailureThresholdK != 7 ||
		c.Spec.Failover.ProbeInterval.Duration != 250*time.Millisecond {
		t.Errorf("explicit values overwritten: %+v", c.Spec)
	}
	if c.Spec.Synchronous != nil {
		t.Errorf("synchronous must stay nil (async) when not requested")
	}
}

func TestInstanceOrdinal(t *testing.T) {
	c := &PostgresCluster{ObjectMeta: metav1.ObjectMeta{Name: "db"}}
	for name, want := range map[string]int{"db-1": 1, "db-12": 12, "db-0": 0, "db-x": 0, "other-1": 0, "db-1-2": 0} {
		got, ok := c.InstanceOrdinal(name)
		if (want == 0) == ok || got != want {
			t.Errorf("InstanceOrdinal(%q) = %d, %v; want %d", name, got, ok, want)
		}
	}
	if c.InstanceName(2) != "db-2" {
		t.Errorf("InstanceName(2) = %s", c.InstanceName(2))
	}
}
