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
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func validCluster() *PostgresCluster {
	c := &PostgresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "db"},
		Spec:       PostgresClusterSpec{Synchronous: &SynchronousSpec{}},
	}
	c.Default()
	return c
}

func TestValidateSpec(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*PostgresCluster)
		wantErr string
	}{
		{name: "defaults are valid", mutate: func(*PostgresCluster) {}},
		{
			name:    "sync number equals instances",
			mutate:  func(c *PostgresCluster) { c.Spec.Instances = 1 },
			wantErr: "spec.synchronous.number",
		},
		{
			name:    "reserved parameter",
			mutate:  func(c *PostgresCluster) { c.Spec.Parameters = map[string]string{"Synchronous_Commit": "off"} },
			wantErr: "spec.parameters[Synchronous_Commit]",
		},
		{
			name:   "free parameter",
			mutate: func(c *PostgresCluster) { c.Spec.Parameters = map[string]string{"work_mem": "8MB"} },
		},
		{
			name: "lease shorter than two probe intervals",
			mutate: func(c *PostgresCluster) {
				c.Spec.Failover.LeaseDuration = metav1.Duration{Duration: 1500 * time.Millisecond}
			},
			wantErr: "spec.failover.leaseDuration",
		},
		{
			name:    "negative probe timeout",
			mutate:  func(c *PostgresCluster) { c.Spec.Failover.ProbeTimeout = metav1.Duration{Duration: -time.Second} },
			wantErr: "spec.failover.probeTimeout",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validCluster()
			tt.mutate(c)
			errs := c.ValidateSpec()
			if tt.wantErr == "" {
				if len(errs) != 0 {
					t.Fatalf("unexpected errors: %v", errs)
				}
				return
			}
			if !strings.Contains(errs.ToAggregate().Error(), tt.wantErr) {
				t.Fatalf("want error on %s, got %v", tt.wantErr, errs)
			}
		})
	}
}

func TestValidateTransition(t *testing.T) {
	sc := func(s string) *string { return &s }
	tests := []struct {
		name    string
		mutate  func(old, cur *PostgresCluster)
		wantErr string
	}{
		{name: "no change", mutate: func(_, _ *PostgresCluster) {}},
		{
			name:    "storage class changed",
			mutate:  func(_, c *PostgresCluster) { c.Spec.Storage.StorageClassName = sc("fast") },
			wantErr: "storageClassName",
		},
		{
			name:    "storage shrunk",
			mutate:  func(_, c *PostgresCluster) { c.Spec.Storage.Size = resource.MustParse("512Mi") },
			wantErr: "spec.storage.size",
		},
		{
			name:   "storage grown",
			mutate: func(_, c *PostgresCluster) { c.Spec.Storage.Size = resource.MustParse("2Gi") },
		},
		{
			name: "image change while promoting",
			mutate: func(o, c *PostgresCluster) {
				o.Status.FailoverPhase = PhasePromoting
				c.Spec.PostgresImage = "postgres:18.7"
			},
			wantErr: "spec.postgresImage",
		},
		{
			name: "image change while healthy",
			mutate: func(o, c *PostgresCluster) {
				o.Status.FailoverPhase = PhaseHealthy
				c.Spec.PostgresImage = "postgres:18.7"
			},
		},
		{
			name: "scale down while fencing",
			mutate: func(o, c *PostgresCluster) {
				o.Status.FailoverPhase = PhaseFencing
				c.Spec.Instances = 2
			},
			wantErr: "spec.instances",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old, cur := validCluster(), validCluster()
			tt.mutate(old, cur)
			errs := cur.ValidateTransition(old)
			if tt.wantErr == "" {
				if len(errs) != 0 {
					t.Fatalf("unexpected errors: %v", errs)
				}
				return
			}
			if !strings.Contains(errs.ToAggregate().Error(), tt.wantErr) {
				t.Fatalf("want error on %s, got %v", tt.wantErr, errs)
			}
		})
	}
}

func TestWebhookWarnings(t *testing.T) {
	v := &postgresClusterValidator{}

	c := validCluster()
	c.Spec.Instances = 2
	w, err := v.ValidateCreate(context.Background(), c)
	if err != nil || len(w) != 1 || !strings.Contains(w[0], "writes block") {
		t.Fatalf("want all-synchronous warning, got %v, %v", w, err)
	}

	c = validCluster()
	c.Spec.Synchronous = nil
	w, _ = v.ValidateCreate(context.Background(), c)
	if len(w) != 1 || !strings.Contains(w[0], "lose acknowledged writes") {
		t.Fatalf("want async warning, got %v", w)
	}
}
