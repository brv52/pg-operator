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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Label and annotation keys shared by the operator, the instance bootstrap
// script and anything that selects PostgreSQL pods (Services, chaos
// experiments, the benchmark harness).
const (
	LabelCluster  = "pg.platform.local/cluster"
	LabelInstance = "pg.platform.local/instance"
	LabelRole     = "pg.platform.local/role"

	// AnnotationEpoch records the failover epoch a pod was created or
	// promoted in. A pod whose epoch is behind the cluster epoch can never
	// be routed to as a primary.
	AnnotationEpoch = "pg.platform.local/epoch"
)

// Role values for LabelRole.
const (
	RolePrimary = "primary"
	RoleReplica = "replica"
	// RoleFenced is applied to a primary that the operator has decided is
	// failed. It matches neither the -rw nor the -ro Service selector.
	RoleFenced = "fenced"
)

// DefaultPostgresImage is the operand image used when spec.postgresImage is
// empty. It is pinned by digest so that a cluster never silently changes
// minor version on pod recreation.
const DefaultPostgresImage = "postgres:18.6-bookworm@sha256:bbf83594798e144bff365a9c191c5d779f181fb3135902064ab8ff11918020fc"

// SyncMethod selects how synchronous_standby_names is rendered.
// +kubebuilder:validation:Enum=any;first
type SyncMethod string

const (
	SyncMethodAny   SyncMethod = "any"
	SyncMethodFirst SyncMethod = "first"
)

// SynchronousSpec configures quorum synchronous replication.
type SynchronousSpec struct {
	// Method is the quorum method: "any" renders ANY n (...), "first"
	// renders FIRST n (...). "any" is required for the failover safety
	// argument in docs/design.md and is the default.
	// +kubebuilder:default=any
	// +optional
	Method SyncMethod `json:"method,omitempty"`

	// Number of standbys that must confirm a commit before it is
	// acknowledged to the client.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	// +optional
	Number int32 `json:"number,omitempty"`
}

// StorageSpec describes the PVC created for every instance.
type StorageSpec struct {
	// Size of each instance's data volume.
	// +kubebuilder:default="1Gi"
	// +optional
	Size resource.Quantity `json:"size,omitempty"`

	// StorageClassName for the data volumes. Empty uses the cluster default.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// FailoverSpec tunes primary failure detection and promotion.
type FailoverSpec struct {
	// ProbeInterval is the period of the SQL health probe against the
	// primary and of the WAL receiver probe against each standby.
	// +kubebuilder:default="1s"
	// +optional
	ProbeInterval metav1.Duration `json:"probeInterval,omitempty"`

	// ProbeTimeout bounds a single probe round trip.
	// +kubebuilder:default="1s"
	// +optional
	ProbeTimeout metav1.Duration `json:"probeTimeout,omitempty"`

	// FailureThresholdK is the number of consecutive failed probe rounds
	// required before the primary is declared failed.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=30
	// +kubebuilder:default=3
	// +optional
	FailureThresholdK int32 `json:"failureThresholdK,omitempty"`

	// LeaseDuration of the coordination Lease that records the primary
	// identity and failover epoch.
	// +kubebuilder:default="15s"
	// +optional
	LeaseDuration metav1.Duration `json:"leaseDuration,omitempty"`
}

// PostgresClusterSpec defines the desired state of PostgresCluster.
// +kubebuilder:validation:XValidation:rule="!has(self.synchronous) || self.synchronous.number < self.instances",message="synchronous.number must be lower than instances"
type PostgresClusterSpec struct {
	// Instances is the number of PostgreSQL pods: one primary and
	// instances-1 streaming standbys.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=9
	// +kubebuilder:default=3
	// +optional
	Instances int32 `json:"instances,omitempty"`

	// PostgresImage is the operand image. It must contain pg_ctl,
	// pg_basebackup, pg_rewind and a POSIX shell.
	// +optional
	PostgresImage string `json:"postgresImage,omitempty"`

	// Synchronous replication settings. When omitted, commits are
	// acknowledged without waiting for a standby (RPO > 0).
	// +optional
	Synchronous *SynchronousSpec `json:"synchronous,omitempty"`

	// Storage for each instance.
	// +kubebuilder:default={}
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`

	// Failover tunables.
	// +kubebuilder:default={}
	// +optional
	Failover FailoverSpec `json:"failover,omitempty"`

	// Resources applied to the postgres container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Parameters are extra postgresql.conf settings. Settings the operator
	// owns (replication, WAL and listen settings) are rejected.
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`
}

// FailoverPhase is a step of the failover state machine. The phase is
// persisted before the side effects of that phase run, so a restarted
// operator can resume from it.
// +kubebuilder:validation:Enum=Healthy;Detecting;Fencing;Promoting;Reconfiguring
type FailoverPhase string

const (
	PhaseHealthy       FailoverPhase = "Healthy"
	PhaseDetecting     FailoverPhase = "Detecting"
	PhaseFencing       FailoverPhase = "Fencing"
	PhasePromoting     FailoverPhase = "Promoting"
	PhaseReconfiguring FailoverPhase = "Reconfiguring"
)

// InstanceStatus is the last observed replication state of one instance.
type InstanceStatus struct {
	Name string `json:"name"`
	// Role mirrors the pod's role label.
	// +optional
	Role string `json:"role,omitempty"`
	// Ready is true when the pod is Ready and answered the last probe.
	Ready bool `json:"ready"`
	// +optional
	Timeline int64 `json:"timeline,omitempty"`
	// ReceiveLSN is pg_last_wal_receive_lsn() on a standby or
	// pg_current_wal_lsn() on the primary.
	// +optional
	ReceiveLSN string `json:"receiveLSN,omitempty"`
	// ReplayLSN is pg_last_wal_replay_lsn() on a standby.
	// +optional
	ReplayLSN string `json:"replayLSN,omitempty"`
	// Streaming is true when a standby's WAL receiver is connected.
	// +optional
	Streaming bool `json:"streaming,omitempty"`
}

// PostgresClusterStatus defines the observed state of PostgresCluster.
type PostgresClusterStatus struct {
	// Phase is a short human-readable summary.
	// +optional
	Phase string `json:"phase,omitempty"`

	// FailoverPhase is the current step of the failover state machine.
	// +optional
	FailoverPhase FailoverPhase `json:"failoverPhase,omitempty"`

	// CurrentPrimary is the instance that holds the primary role.
	// +optional
	CurrentPrimary string `json:"currentPrimary,omitempty"`

	// TargetPrimary is the instance chosen for promotion during a
	// failover. It equals CurrentPrimary outside a failover.
	// +optional
	TargetPrimary string `json:"targetPrimary,omitempty"`

	// Epoch increases by one on every failover and is mirrored into the
	// coordination Lease and the primary pod annotation.
	// +optional
	Epoch int64 `json:"epoch,omitempty"`

	// ProbeFailures counts consecutive failed primary probe rounds.
	// +optional
	ProbeFailures int32 `json:"probeFailures,omitempty"`

	// FailoverStartTime is when the current failover left Healthy.
	// +optional
	FailoverStartTime *metav1.Time `json:"failoverStartTime,omitempty"`

	// LastFailoverDuration is the operator-side duration of the last
	// completed failover (detection excluded).
	// +optional
	LastFailoverDuration *metav1.Duration `json:"lastFailoverDuration,omitempty"`

	// ReadyInstances is the number of instances that are Ready.
	// +optional
	ReadyInstances int32 `json:"readyInstances,omitempty"`

	// Instances lists per-instance replication state.
	// +listType=map
	// +listMapKey=name
	// +optional
	Instances []InstanceStatus `json:"instances,omitempty"`

	// ObservedGeneration is the spec generation the status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Condition types.
const (
	ConditionReady    = "Ready"
	ConditionFailover = "FailoverInProgress"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.instances,statuspath=.status.readyInstances
// +kubebuilder:resource:shortName=pgc
// +kubebuilder:printcolumn:name="Instances",type=integer,JSONPath=`.spec.instances`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyInstances`
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=`.status.currentPrimary`
// +kubebuilder:printcolumn:name="Failover",type=string,JSONPath=`.status.failoverPhase`
// +kubebuilder:printcolumn:name="Epoch",type=integer,JSONPath=`.status.epoch`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PostgresCluster is a highly available PostgreSQL cluster with one primary
// and quorum-synchronous standbys.
type PostgresCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PostgresCluster
	// +required
	Spec PostgresClusterSpec `json:"spec"`

	// status defines the observed state of PostgresCluster
	// +optional
	Status PostgresClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PostgresClusterList contains a list of PostgresCluster
type PostgresClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PostgresCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PostgresCluster{}, &PostgresClusterList{})
		return nil
	})
}
