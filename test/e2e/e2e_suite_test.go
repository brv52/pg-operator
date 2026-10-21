//go:build e2e

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

// Package e2e runs the operator against a real cluster: the k3d cluster
// from hack/k3d-config.yaml with the operator and the checker image
// deployed by make deploy-local (make test-e2e does both). Each run works
// in its own namespace and removes it at the end, so the suite can be run
// repeatedly against a long-lived development cluster.
package e2e

import (
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/brv52/pg-operator/test/utils"
)

const operatorNamespace = "pg-operator-system"

// namespace is unique per run.
var namespace = fmt.Sprintf("pg-e2e-%d", time.Now().Unix()%100000)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting pg-operator e2e suite in namespace %s\n", namespace)
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	// Do not let a developer's kuberc aliases or defaults change kubectl
	// output the suite parses.
	if os.Getenv("KUBECTL_KUBERC") == "" {
		Expect(os.Setenv("KUBECTL_KUBERC", "false")).To(Succeed())
	}

	By("checking that the operator is running")
	_, err := utils.Kubectl("-n", operatorNamespace, "rollout", "status",
		"deploy/pg-operator-controller-manager", "--timeout=120s")
	Expect(err).NotTo(HaveOccurred(), "deploy the operator first: make deploy-local")

	By("creating namespace " + namespace)
	_, err = utils.Kubectl("create", "namespace", namespace)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if os.Getenv("E2E_KEEP_NAMESPACE") != "" {
		return
	}
	By("deleting namespace " + namespace)
	_, _ = utils.Kubectl("delete", "namespace", namespace, "--wait=false")
})
