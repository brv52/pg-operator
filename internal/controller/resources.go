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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	pgv1 "github.com/brv52/pg-operator/api/v1alpha1"
	"github.com/brv52/pg-operator/internal/pg"
)

const (
	dataMount = "/var/lib/postgresql/data"
	pgdata    = dataMount + "/pgdata"

	secretSuperuserKey   = "superuser-password"
	secretReplicationKey = "replication-password"

	// annotationSpecHash records the pod-template inputs a pod was built
	// from, so drift (image, resources) is detected without diffing specs.
	annotationSpecHash = "pg.platform.local/spec-hash"
)

func ownerRef(c *pgv1.PostgresCluster) metav1.OwnerReference {
	return *metav1.NewControllerRef(c, pgv1.GroupVersion.WithKind("PostgresCluster"))
}

func objectMeta(c *pgv1.PostgresCluster, name string, extra map[string]string) metav1.ObjectMeta {
	labels := c.SelectorLabels()
	labels["app.kubernetes.io/name"] = "postgresql"
	labels["app.kubernetes.io/managed-by"] = "pg-operator"
	maps.Copy(labels, extra)
	return metav1.ObjectMeta{
		Name:            name,
		Namespace:       c.Namespace,
		Labels:          labels,
		OwnerReferences: []metav1.OwnerReference{ownerRef(c)},
	}
}

// instanceHost is the stable DNS name of an instance through the headless
// Service. Replication connections use it rather than the -rw Service so a
// standby's upstream is always an explicit, operator-chosen instance.
func instanceHost(c *pgv1.PostgresCluster, instance string) string {
	return fmt.Sprintf("%s.%s.%s.svc", instance, c.HeadlessServiceName(), c.Namespace)
}

func randomPassword() (string, error) {
	const alphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	b := make([]byte, 32)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			return "", err
		}
		b[i] = alphabet[n.Int64()]
	}
	return string(b), nil
}

func buildSecret(c *pgv1.PostgresCluster) (*corev1.Secret, error) {
	su, err := randomPassword()
	if err != nil {
		return nil, err
	}
	repl, err := randomPassword()
	if err != nil {
		return nil, err
	}
	return &corev1.Secret{
		ObjectMeta: objectMeta(c, c.SecretName(), nil),
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"username":           pg.SuperUser,
			secretSuperuserKey:   su,
			secretReplicationKey: repl,
		},
	}, nil
}

// renderCustomConf turns spec.parameters into an include file. It is
// reloaded, not restarted, when it changes.
func renderCustomConf(c *pgv1.PostgresCluster) string {
	keys := slices.Sorted(maps.Keys(c.Spec.Parameters))
	var b strings.Builder
	b.WriteString("# managed by pg-operator from spec.parameters\n")
	for _, k := range keys {
		_, _ = fmt.Fprintf(&b, "%s = %s\n", k, pg.QuoteLiteral(c.Spec.Parameters[k]))
	}
	return b.String()
}

func buildConfigMap(c *pgv1.PostgresCluster) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: objectMeta(c, c.ConfigMapName(), nil),
		Data: map[string]string{
			"instance.sh": pg.InstanceScript,
			"pg_hba.conf": pg.HBA,
			"custom.conf": renderCustomConf(c),
		},
	}
}

func buildServices(c *pgv1.PostgresCluster) []*corev1.Service {
	port := []corev1.ServicePort{{Name: "postgres", Port: pg.Port, TargetPort: intstr.FromInt32(pg.Port)}}
	withRole := func(role string) map[string]string {
		s := c.SelectorLabels()
		s[pgv1.LabelRole] = role
		return s
	}
	headless := &corev1.Service{
		ObjectMeta: objectMeta(c, c.HeadlessServiceName(), nil),
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  c.SelectorLabels(),
			Ports:     port,
			// Standbys being cloned must resolve their upstream, and the
			// upstream must resolve before it is Ready during bootstrap.
			PublishNotReadyAddresses: true,
		},
	}
	rw := &corev1.Service{
		ObjectMeta: objectMeta(c, c.ReadWriteServiceName(), nil),
		Spec:       corev1.ServiceSpec{Selector: withRole(pgv1.RolePrimary), Ports: port},
	}
	ro := &corev1.Service{
		ObjectMeta: objectMeta(c, c.ReadOnlyServiceName(), nil),
		Spec:       corev1.ServiceSpec{Selector: withRole(pgv1.RoleReplica), Ports: port},
	}
	return []*corev1.Service{headless, rw, ro}
}

// buildPDBs protects the primary from voluntary eviction entirely. A node
// drain therefore blocks on the primary until the operator has switched
// over, which relabels the old primary and releases it from this budget.
func buildPDBs(c *pgv1.PostgresCluster) []*policyv1.PodDisruptionBudget {
	sel := func(role string) *metav1.LabelSelector {
		l := c.SelectorLabels()
		l[pgv1.LabelRole] = role
		return &metav1.LabelSelector{MatchLabels: l}
	}
	one := intstr.FromInt32(1)
	primary := &policyv1.PodDisruptionBudget{
		ObjectMeta: objectMeta(c, c.Name+"-primary", nil),
		Spec:       policyv1.PodDisruptionBudgetSpec{Selector: sel(pgv1.RolePrimary), MinAvailable: &one},
	}
	replicas := &policyv1.PodDisruptionBudget{
		ObjectMeta: objectMeta(c, c.Name+"-replicas", nil),
		Spec:       policyv1.PodDisruptionBudgetSpec{Selector: sel(pgv1.RoleReplica), MaxUnavailable: &one},
	}
	return []*policyv1.PodDisruptionBudget{primary, replicas}
}

func buildPVC(c *pgv1.PostgresCluster, name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: objectMeta(c, name, map[string]string{pgv1.LabelInstance: name}),
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: c.Spec.Storage.StorageClassName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: c.Spec.Storage.Size},
			},
		},
	}
}

// podSpecHash covers the inputs whose change requires recreating a pod.
func podSpecHash(c *pgv1.PostgresCluster) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s\n%s\n", c.Spec.PostgresImage, c.Spec.Resources.String())
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// buildPod creates an instance pod. role is fixed for the pod's lifetime as
// far as the bootstrap script is concerned; primaryInstance is the clone
// and rewind source for replicas.
func buildPod(c *pgv1.PostgresCluster, name, role, primaryInstance string, epoch int64) *corev1.Pod {
	meta := objectMeta(c, name, map[string]string{
		pgv1.LabelInstance: name,
		pgv1.LabelRole:     role,
	})
	meta.Annotations = map[string]string{
		pgv1.AnnotationEpoch: fmt.Sprint(epoch),
		annotationSpecHash:   podSpecHash(c),
	}

	scriptRole := pgv1.RoleReplica
	if role == pgv1.RolePrimary {
		scriptRole = pgv1.RolePrimary
	}
	secretEnv := func(name, key string) corev1.EnvVar {
		return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			Name: c.SecretName(), Key: key,
		}}}
	}
	env := []corev1.EnvVar{
		{Name: "POD_NAME", Value: name},
		{Name: "ROLE", Value: scriptRole},
		{Name: "PRIMARY_HOST", Value: instanceHost(c, primaryInstance)},
		{Name: "PGDATA", Value: pgdata},
		{Name: "SLOT_NAME", Value: pg.SlotName(name)},
		{Name: "STATIC_FLAGS", Value: strings.Join(pg.StaticFlags(), " ")},
		secretEnv("SUPERUSER_PASSWORD", secretSuperuserKey),
		secretEnv("REPLICATION_PASSWORD", secretReplicationKey),
	}
	mounts := []corev1.VolumeMount{
		{Name: "data", MountPath: dataMount},
		{Name: "scripts", MountPath: "/scripts"},
		{Name: "config", MountPath: "/etc/pg"},
		{Name: "podinfo", MountPath: "/etc/podinfo"},
		{Name: "tmp", MountPath: "/tmp"},
		{Name: "run", MountPath: "/var/run/postgresql"},
	}
	secCtx := &corev1.SecurityContext{
		AllowPrivilegeEscalation: new(false),
		ReadOnlyRootFilesystem:   new(true),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
	isReady := corev1.ProbeHandler{Exec: &corev1.ExecAction{
		Command: []string{"pg_isready", "-q", "-h", "/tmp", "-p", fmt.Sprint(pg.Port), "-U", pg.SuperUser},
	}}

	antiAffinity := &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			LabelSelector: &metav1.LabelSelector{MatchLabels: c.SelectorLabels()},
			TopologyKey:   corev1.LabelHostname,
		}},
	}}

	return &corev1.Pod{
		ObjectMeta: meta,
		Spec: corev1.PodSpec{
			Hostname:  name,
			Subdomain: c.HeadlessServiceName(),
			Affinity:  antiAffinity,
			// Long enough for a fast shutdown to ship the final WAL to the
			// standbys during a switchover.
			TerminationGracePeriodSeconds: new(int64(30)),
			EnableServiceLinks:            new(false),
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser:           new(int64(999)),
				RunAsGroup:          new(int64(999)),
				FSGroup:             new(int64(999)),
				RunAsNonRoot:        new(true),
				FSGroupChangePolicy: new(corev1.FSGroupChangeOnRootMismatch),
				SeccompProfile:      &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			InitContainers: []corev1.Container{{
				Name:            "bootstrap",
				Image:           c.Spec.PostgresImage,
				Command:         []string{"sh", "/scripts/instance.sh", "init"},
				Env:             env,
				VolumeMounts:    mounts,
				SecurityContext: secCtx,
				Resources:       c.Spec.Resources,
			}},
			Containers: []corev1.Container{{
				Name:            "postgres",
				Image:           c.Spec.PostgresImage,
				Command:         []string{"sh", "/scripts/instance.sh", "run"},
				Env:             env,
				Ports:           []corev1.ContainerPort{{Name: "postgres", ContainerPort: pg.Port}},
				VolumeMounts:    append(slices.Clone(mounts), corev1.VolumeMount{Name: "shm", MountPath: "/dev/shm"}),
				SecurityContext: secCtx,
				Resources:       c.Spec.Resources,
				StartupProbe: &corev1.Probe{
					ProbeHandler: isReady, PeriodSeconds: 2, FailureThreshold: 900,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: isReady, PeriodSeconds: 2, FailureThreshold: 2, TimeoutSeconds: 2,
				},
				// No liveness probe: restarting postgres is a failover
				// decision, and that belongs to the operator.
				Lifecycle: &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{
					Command: []string{"pg_ctl", "-D", pgdata, "-m", "fast", "-w", "-t", "25", "stop"},
				}}},
			}},
			Volumes: []corev1.Volume{
				{Name: "data",
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: name}},
				{Name: "scripts", ConfigMap: &corev1.ConfigMapVolumeSource{
					Name:  c.ConfigMapName(),
					Items: []corev1.KeyToPath{{Key: "instance.sh", Path: "instance.sh", Mode: new(int32(0o555))}},
				}},
				{Name: "config", ConfigMap: &corev1.ConfigMapVolumeSource{
					Name: c.ConfigMapName(),
					Items: []corev1.KeyToPath{
						{Key: "pg_hba.conf", Path: "pg_hba.conf"},
						{Key: "custom.conf", Path: "custom.conf"},
					},
				}},
				{Name: "podinfo", DownwardAPI: &corev1.DownwardAPIVolumeSource{
					Items: []corev1.DownwardAPIVolumeFile{{Path: "labels", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.labels"}}},
				}},
				{Name: "tmp", EmptyDir: &corev1.EmptyDirVolumeSource{}},
				{Name: "run", EmptyDir: &corev1.EmptyDirVolumeSource{}},
				{Name: "shm", EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium: corev1.StorageMediumMemory,
				}},
			},
		},
	}
}
