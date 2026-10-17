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

// Package webhookcert provisions the TLS certificate for the admission
// webhooks without cert-manager.
//
// The serving certificate and its CA live in a Secret, so every operator
// replica serves the same certificate and none of them fights the others
// over the caBundle. On startup each replica loads the Secret (creating or
// renewing it when needed), writes the key pair to the webhook server's
// cert directory and makes sure both webhook configurations trust the CA.
package webhookcert

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	keyCACert = "ca.crt"
	keyCert   = corev1.TLSCertKey
	keyKey    = corev1.TLSPrivateKeyKey

	validity = 365 * 24 * time.Hour
	// renewBefore triggers a new certificate at startup when the current
	// one expires sooner than this. Operator pods restart far more often
	// than once a month in practice; the margin covers the case where
	// they do not.
	renewBefore = 30 * 24 * time.Hour
)

// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=mutatingwebhookconfigurations;validatingwebhookconfigurations,verbs=get;list;watch;update;patch

// Options locates the objects the certificate is for.
type Options struct {
	Namespace   string
	SecretName  string
	ServiceName string
	// MutatingWebhookConfiguration and ValidatingWebhookConfiguration are
	// the object names whose caBundle is injected. Empty names are skipped.
	MutatingWebhookConfiguration   string
	ValidatingWebhookConfiguration string
	// CertDir receives tls.crt and tls.key.
	CertDir string
	// Now is the clock; tests replace it.
	Now func() time.Time
}

func (o Options) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

// Ensure makes the certificate Secret valid, writes the key pair to
// CertDir and injects the CA into the webhook configurations. It must run
// before the webhook server starts and uses a client that reads straight
// from the API server (the manager cache is not running yet).
func Ensure(ctx context.Context, c client.Client, o Options) error {
	secret, err := ensureSecret(ctx, c, o)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(o.CertDir, 0o700); err != nil {
		return err
	}
	for name, key := range map[string]string{keyCert: keyCert, keyKey: keyKey} {
		if err := os.WriteFile(filepath.Join(o.CertDir, name), secret.Data[key], 0o600); err != nil {
			return err
		}
	}
	return injectCABundle(ctx, c, o, secret.Data[keyCACert])
}

func ensureSecret(ctx context.Context, c client.Client, o Options) (*corev1.Secret, error) {
	for range 5 {
		var s corev1.Secret
		err := c.Get(ctx, client.ObjectKey{Namespace: o.Namespace, Name: o.SecretName}, &s)
		switch {
		case apierrors.IsNotFound(err):
			s = corev1.Secret{Namespace: o.Namespace, Name: o.SecretName, Type: corev1.SecretTypeTLS}
			if s.Data, err = generate(o); err != nil {
				return nil, err
			}
			err = c.Create(ctx, &s)
			if apierrors.IsAlreadyExists(err) {
				continue // another replica won the race; use its certificate
			}
			return &s, err
		case err != nil:
			return nil, err
		}

		if valid(s.Data, o) {
			return &s, nil
		}
		if s.Data, err = generate(o); err != nil {
			return nil, err
		}
		s.Type = corev1.SecretTypeTLS
		err = c.Update(ctx, &s)
		if apierrors.IsConflict(err) {
			continue
		}
		return &s, err
	}
	return nil, errors.New("webhook certificate secret kept changing underneath us")
}

func dnsNames(o Options) []string {
	return []string{
		fmt.Sprintf("%s.%s.svc", o.ServiceName, o.Namespace),
		fmt.Sprintf("%s.%s.svc.cluster.local", o.ServiceName, o.Namespace),
	}
}

// valid reports whether data holds a key pair for the expected DNS names
// that chains to the stored CA and is not close to expiry.
func valid(data map[string][]byte, o Options) bool {
	caBlock, _ := pem.Decode(data[keyCACert])
	certBlock, _ := pem.Decode(data[keyCert])
	if caBlock == nil || certBlock == nil || len(data[keyKey]) == 0 {
		return false
	}
	ca, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		return false
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return false
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	_, err = cert.Verify(x509.VerifyOptions{
		DNSName:     dnsNames(o)[0],
		Roots:       pool,
		CurrentTime: o.now().Add(renewBefore),
	})
	return err == nil
}

// generate creates a CA and a serving certificate signed by it.
func generate(o Options) (map[string][]byte, error) {
	now := o.now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "pg-operator-webhook-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(validity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, err
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	names := dnsNames(o)
	tmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: names[0]},
		DNSNames:     names,
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(validity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	return map[string][]byte{
		keyCACert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		keyCert:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyKey:    pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	return n
}

func injectCABundle(ctx context.Context, c client.Client, o Options, ca []byte) error {
	if name := o.MutatingWebhookConfiguration; name != "" {
		var m admissionregistrationv1.MutatingWebhookConfiguration
		if err := c.Get(ctx, client.ObjectKey{Name: name}, &m); err != nil {
			return fmt.Errorf("mutating webhook configuration %s: %w", name, err)
		}
		patch := client.MergeFrom(m.DeepCopy())
		changed := false
		for i := range m.Webhooks {
			if !bytes.Equal(m.Webhooks[i].ClientConfig.CABundle, ca) {
				m.Webhooks[i].ClientConfig.CABundle = ca
				changed = true
			}
		}
		if changed {
			if err := c.Patch(ctx, &m, patch); err != nil {
				return err
			}
		}
	}
	if name := o.ValidatingWebhookConfiguration; name != "" {
		var v admissionregistrationv1.ValidatingWebhookConfiguration
		if err := c.Get(ctx, client.ObjectKey{Name: name}, &v); err != nil {
			return fmt.Errorf("validating webhook configuration %s: %w", name, err)
		}
		patch := client.MergeFrom(v.DeepCopy())
		changed := false
		for i := range v.Webhooks {
			if !bytes.Equal(v.Webhooks[i].ClientConfig.CABundle, ca) {
				v.Webhooks[i].ClientConfig.CABundle = ca
				changed = true
			}
		}
		if changed {
			if err := c.Patch(ctx, &v, patch); err != nil {
				return err
			}
		}
	}
	return nil
}
