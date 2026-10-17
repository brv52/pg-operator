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

package webhookcert

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func setup(t *testing.T) (client.Client, Options) {
	c := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(
		&admissionregistrationv1.MutatingWebhookConfiguration{
			Name:     "mwc",
			Webhooks: []admissionregistrationv1.MutatingWebhook{{Name: "m.example.com"}},
		},
		&admissionregistrationv1.ValidatingWebhookConfiguration{
			Name:     "vwc",
			Webhooks: []admissionregistrationv1.ValidatingWebhook{{Name: "v.example.com"}},
		},
	).Build()
	return c, Options{
		Namespace: "ops", SecretName: "webhook-cert", ServiceName: "webhook",
		MutatingWebhookConfiguration: "mwc", ValidatingWebhookConfiguration: "vwc",
		CertDir: t.TempDir(),
	}
}

func TestEnsureCreatesAndInjects(t *testing.T) {
	ctx := context.Background()
	c, o := setup(t)
	if err := Ensure(ctx, c, o); err != nil {
		t.Fatal(err)
	}

	if _, err := tls.LoadX509KeyPair(filepath.Join(o.CertDir, "tls.crt"), filepath.Join(o.CertDir, "tls.key")); err != nil {
		t.Fatalf("written key pair unusable: %v", err)
	}
	var s corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ops", Name: "webhook-cert"}, &s); err != nil {
		t.Fatal(err)
	}
	if !valid(s.Data, o) {
		t.Fatal("generated certificate does not verify for the service DNS name")
	}
	var m admissionregistrationv1.MutatingWebhookConfiguration
	_ = c.Get(ctx, client.ObjectKey{Name: "mwc"}, &m)
	var v admissionregistrationv1.ValidatingWebhookConfiguration
	_ = c.Get(ctx, client.ObjectKey{Name: "vwc"}, &v)
	if string(m.Webhooks[0].ClientConfig.CABundle) != string(s.Data["ca.crt"]) ||
		string(v.Webhooks[0].ClientConfig.CABundle) != string(s.Data["ca.crt"]) {
		t.Fatal("caBundle not injected")
	}
}

func TestEnsureReusesValidCertificate(t *testing.T) {
	ctx := context.Background()
	c, o := setup(t)
	if err := Ensure(ctx, c, o); err != nil {
		t.Fatal(err)
	}
	first, _ := os.ReadFile(filepath.Join(o.CertDir, "tls.crt"))

	// A second replica starting later must serve the same certificate.
	o2 := o
	o2.CertDir = t.TempDir()
	if err := Ensure(ctx, c, o2); err != nil {
		t.Fatal(err)
	}
	second, _ := os.ReadFile(filepath.Join(o2.CertDir, "tls.crt"))
	if string(first) != string(second) {
		t.Fatal("replicas ended up with different certificates")
	}
}

func TestEnsureRenewsNearExpiry(t *testing.T) {
	ctx := context.Background()
	c, o := setup(t)
	if err := Ensure(ctx, c, o); err != nil {
		t.Fatal(err)
	}
	first, _ := os.ReadFile(filepath.Join(o.CertDir, "tls.crt"))

	o.Now = func() time.Time { return time.Now().Add(validity - renewBefore/2) }
	if err := Ensure(ctx, c, o); err != nil {
		t.Fatal(err)
	}
	second, _ := os.ReadFile(filepath.Join(o.CertDir, "tls.crt"))
	if string(first) == string(second) {
		t.Fatal("certificate inside the renewal window was not replaced")
	}
}

func TestValidRejectsWrongService(t *testing.T) {
	_, o := setup(t)
	data, err := generate(o)
	if err != nil {
		t.Fatal(err)
	}
	o.ServiceName = "other"
	if valid(data, o) {
		t.Fatal("certificate for another service accepted")
	}
}
