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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	pgv1 "github.com/brv52/pg-operator/api/v1alpha1"
)

func testCluster() *pgv1.PostgresCluster {
	c := &pgv1.PostgresCluster{Name: "db", Namespace: "ns"}
	c.Default()
	return c
}

func TestBuildPodInvariants(t *testing.T) {
	c := testCluster()
	p := buildPod(c, "db-2", pgv1.RoleReplica, "db-1", 4)

	if p.Labels[pgv1.LabelRole] != pgv1.RoleReplica || p.Labels[pgv1.LabelCluster] != "db" {
		t.Fatalf("labels = %v", p.Labels)
	}
	if p.Annotations[pgv1.AnnotationEpoch] != "4" {
		t.Errorf("epoch annotation = %q", p.Annotations[pgv1.AnnotationEpoch])
	}
	if p.Spec.Hostname != "db-2" || p.Spec.Subdomain != "db-instances" {
		t.Errorf("hostname/subdomain = %s/%s", p.Spec.Hostname, p.Spec.Subdomain)
	}
	terms := p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 || terms[0].TopologyKey != corev1.LabelHostname {
		t.Errorf("instances must be spread one per node: %+v", terms)
	}

	pgc := p.Spec.Containers[0]
	if pgc.LivenessProbe != nil {
		t.Error("a liveness probe would let the kubelet restart a primary behind the operator's back")
	}
	if pgc.Lifecycle == nil || !strings.Contains(strings.Join(pgc.Lifecycle.PreStop.Exec.Command, " "), "-m fast") {
		t.Error("preStop must do a fast shutdown; SIGTERM alone is a smart shutdown that waits for clients")
	}
	if sc := pgc.SecurityContext; sc == nil || !*sc.ReadOnlyRootFilesystem || *sc.AllowPrivilegeEscalation {
		t.Errorf("container security context too permissive: %+v", sc)
	}
	env := map[string]string{}
	for _, e := range p.Spec.InitContainers[0].Env {
		env[e.Name] = e.Value
	}
	if env["ROLE"] != "replica" || env["PRIMARY_HOST"] != "db-1.db-instances.ns.svc" || env["SLOT_NAME"] != "pgop_db_2" {
		t.Errorf("bootstrap env = %v", env)
	}
}

func TestFencedPodBootsAsReplica(t *testing.T) {
	p := buildPod(testCluster(), "db-1", pgv1.RoleFenced, "db-2", 3)
	for _, e := range p.Spec.InitContainers[0].Env {
		if e.Name == "ROLE" && e.Value != "replica" {
			t.Fatalf("a pod created with role %s must bootstrap as replica, got %s", pgv1.RoleFenced, e.Value)
		}
	}
}

func TestSpecHashTracksImage(t *testing.T) {
	a, b := testCluster(), testCluster()
	b.Spec.PostgresImage = "postgres:18.7"
	if podSpecHash(a) == podSpecHash(b) {
		t.Error("image change must change the pod spec hash")
	}
	b = testCluster()
	b.Spec.Parameters = map[string]string{"work_mem": "8MB"}
	if podSpecHash(a) != podSpecHash(b) {
		t.Error("parameters are reloaded, they must not roll pods")
	}
}

func TestServicesSelectByRole(t *testing.T) {
	svcs := buildServices(testCluster())
	got := map[string]map[string]string{}
	for _, s := range svcs {
		got[s.Name] = s.Spec.Selector
	}
	if got["db-rw"][pgv1.LabelRole] != pgv1.RolePrimary || got["db-ro"][pgv1.LabelRole] != pgv1.RoleReplica {
		t.Errorf("selectors = %v", got)
	}
	if _, ok := got["db-instances"][pgv1.LabelRole]; ok {
		t.Error("headless service must select every instance")
	}
}
