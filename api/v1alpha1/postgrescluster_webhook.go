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
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// reservedParameters are postgresql.conf settings that the operator renders
// itself. Letting users override them would break replication, the
// synchronous commit guarantee or the rewind path.
var reservedParameters = map[string]struct{}{
	"listen_addresses":          {},
	"port":                      {},
	"hot_standby":               {},
	"wal_level":                 {},
	"wal_log_hints":             {},
	"max_wal_senders":           {},
	"max_replication_slots":     {},
	"synchronous_commit":        {},
	"synchronous_standby_names": {},
	"primary_conninfo":          {},
	"primary_slot_name":         {},
	"recovery_target_timeline":  {},
	"restore_command":           {},
	"data_directory":            {},
	"hba_file":                  {},
}

// SetupWebhookWithManager registers the defaulting and validating webhooks.
func (c *PostgresCluster) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &PostgresCluster{}).
		WithDefaulter(&postgresClusterDefaulter{}).
		WithValidator(&postgresClusterValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-pg-platform-local-v1alpha1-postgrescluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=pg.platform.local,resources=postgresclusters,verbs=create;update,versions=v1alpha1,name=mpostgrescluster-v1alpha1.kb.io,admissionReviewVersions=v1

type postgresClusterDefaulter struct{}

func (*postgresClusterDefaulter) Default(_ context.Context, c *PostgresCluster) error {
	c.Default()
	return nil
}

// +kubebuilder:webhook:path=/validate-pg-platform-local-v1alpha1-postgrescluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=pg.platform.local,resources=postgresclusters,verbs=create;update,versions=v1alpha1,name=vpostgrescluster-v1alpha1.kb.io,admissionReviewVersions=v1

type postgresClusterValidator struct{}

func (*postgresClusterValidator) ValidateCreate(_ context.Context, c *PostgresCluster) (admission.Warnings, error) {
	return c.warnings(), c.toError(c.ValidateSpec())
}

func (*postgresClusterValidator) ValidateUpdate(_ context.Context, oldC, newC *PostgresCluster) (admission.Warnings, error) {
	errs := newC.ValidateSpec()
	errs = append(errs, newC.ValidateTransition(oldC)...)
	return newC.warnings(), newC.toError(errs)
}

func (*postgresClusterValidator) ValidateDelete(context.Context, *PostgresCluster) (admission.Warnings, error) {
	return nil, nil
}

// ValidateSpec checks invariants that the CRD schema cannot express.
func (c *PostgresCluster) ValidateSpec() field.ErrorList {
	var errs field.ErrorList
	spec := field.NewPath("spec")

	if s := c.Spec.Synchronous; s != nil && s.Number >= c.Spec.Instances {
		errs = append(errs, field.Invalid(spec.Child("synchronous", "number"), s.Number,
			fmt.Sprintf("must be lower than instances (%d): the primary cannot be its own synchronous standby", c.Spec.Instances)))
	}

	f := c.Spec.Failover
	fp := spec.Child("failover")
	if f.ProbeTimeout.Duration <= 0 {
		errs = append(errs, field.Invalid(fp.Child("probeTimeout"), f.ProbeTimeout.String(), "must be positive"))
	}
	if f.ProbeInterval.Duration <= 0 {
		errs = append(errs, field.Invalid(fp.Child("probeInterval"), f.ProbeInterval.String(), "must be positive"))
	}
	if f.LeaseDuration.Duration < 2*f.ProbeInterval.Duration {
		errs = append(errs, field.Invalid(fp.Child("leaseDuration"), f.LeaseDuration.String(),
			"must be at least twice probeInterval so a healthy primary renews before expiry"))
	}

	for k := range c.Spec.Parameters {
		if _, ok := reservedParameters[strings.ToLower(k)]; ok {
			errs = append(errs, field.Forbidden(spec.Child("parameters").Key(k), "managed by the operator"))
		}
	}
	return errs
}

// ValidateTransition checks that an update is safe given the previous
// object, including its status.
func (c *PostgresCluster) ValidateTransition(old *PostgresCluster) field.ErrorList {
	var errs field.ErrorList
	storage := field.NewPath("spec", "storage")

	if !equalStringPtr(old.Spec.Storage.StorageClassName, c.Spec.Storage.StorageClassName) {
		errs = append(errs, field.Forbidden(storage.Child("storageClassName"), "is immutable"))
	}
	if c.Spec.Storage.Size.Cmp(old.Spec.Storage.Size) < 0 {
		errs = append(errs, field.Forbidden(storage.Child("size"), "cannot be reduced"))
	}

	// Changing the image rolls every pod. Doing that while the state
	// machine is fencing or promoting would delete the promotion target.
	inFailover := old.Status.FailoverPhase != "" && old.Status.FailoverPhase != PhaseHealthy
	if inFailover && old.Spec.PostgresImage != c.Spec.PostgresImage {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "postgresImage"),
			fmt.Sprintf("cannot change while failover is in phase %s", old.Status.FailoverPhase)))
	}
	if inFailover && c.Spec.Instances < old.Spec.Instances {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "instances"),
			fmt.Sprintf("cannot scale down while failover is in phase %s", old.Status.FailoverPhase)))
	}
	return errs
}

func (c *PostgresCluster) warnings() admission.Warnings {
	var w admission.Warnings
	if s := c.Spec.Synchronous; s != nil && c.Spec.Instances-1 == s.Number {
		w = append(w, fmt.Sprintf("every standby is synchronous (instances=%d, synchronous.number=%d): "+
			"writes block if any standby is unavailable", c.Spec.Instances, s.Number))
	}
	if c.Spec.Synchronous == nil && c.Spec.Instances > 1 {
		w = append(w, "synchronous replication is disabled: a failover can lose acknowledged writes")
	}
	return w
}

func (c *PostgresCluster) toError(errs field.ErrorList) error {
	if len(errs) == 0 {
		return nil
	}
	return apierrors.NewInvalid(GroupVersion.WithKind("PostgresCluster").GroupKind(), c.Name, errs)
}

func equalStringPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
