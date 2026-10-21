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

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/brv52/pg-operator/test/utils"
)

const cluster = "e2e"

// passfile is written by the instance script on every volume; it holds
// the superuser password, so psql inside an instance pod can reach the
// Services without the test handling secrets.
const passfile = "/var/lib/postgresql/data/.pgpass"

func clusterManifest(syncNumber, storage string) string {
	return fmt.Sprintf(`apiVersion: pg.platform.local/v1alpha1
kind: PostgresCluster
metadata:
  name: %s
  namespace: %s
spec:
  instances: 3
  synchronous: {method: any, number: %s}
  storage: {size: %s}
  failover: {probeInterval: 1s, probeTimeout: 1s, failureThresholdK: 3, leaseDuration: 15s}
  resources:
    requests: {cpu: 100m, memory: 128Mi}
    limits: {memory: 256Mi}
  parameters: {shared_buffers: 32MB, max_connections: "60"}
`, cluster, namespace, syncNumber, storage)
}

func status(path string) string {
	GinkgoHelper()
	out, err := utils.Kubectl("-n", namespace, "get", "pgc", cluster, "-o", "jsonpath={.status."+path+"}")
	Expect(err).NotTo(HaveOccurred())
	return out
}

func healthy() bool {
	return status("failoverPhase") == "Healthy" && status("readyInstances") == "3"
}

// psql runs sql in the postgres container of pod against conninfo
// ("" means the local instance).
func psql(pod, conninfo, sql string) (string, error) {
	if conninfo == "" {
		conninfo = "host=/tmp"
	}
	out, err := utils.Kubectl("-n", namespace, "exec", pod, "-c", "postgres", "--",
		"psql", conninfo+" user=postgres dbname=postgres passfile="+passfile+" connect_timeout=3", "-XAtc", sql)
	return strings.TrimSpace(out), err
}

func service(suffix string) string {
	return fmt.Sprintf("host=%s-%s.%s.svc", cluster, suffix, namespace)
}

// checkerResult is the part of the checker's JSON line the suite asserts on.
type checkerResult struct {
	Ops             int     `json:"ops"`
	AckedWrites     int     `json:"ackedWrites"`
	Linearizability string  `json:"linearizability"`
	LostWrites      int     `json:"lostWrites"`
	PhantomWrites   int     `json:"phantomWrites"`
	Passed          bool    `json:"passed"`
	MaxWriteGapMs   float64 `json:"maxWriteGapMs"`
}

func startChecker(run string, duration time.Duration) {
	GinkgoHelper()
	dir, err := utils.GetProjectDir()
	Expect(err).NotTo(HaveOccurred())
	tmpl, err := os.ReadFile(filepath.Join(dir, "test/checker/job.yaml"))
	Expect(err).NotTo(HaveOccurred())
	vars := map[string]string{
		"NAMESPACE": namespace, "SECRET": cluster + "-credentials", "SECRET_KEY": "superuser-password",
		"RW_SERVICE": cluster + "-rw", "RUN_ID": run, "DURATION": duration.String(),
		"KEY_BASE": strconv.FormatInt(time.Now().UnixNano()/1000, 10),
	}
	manifest := os.Expand(string(tmpl), func(k string) string { return vars[k] })
	_, err = utils.KubectlStdin(manifest, "apply", "-f", "-")
	Expect(err).NotTo(HaveOccurred())
	_, err = utils.Kubectl("-n", namespace, "wait", "--for=condition=Ready", "pod",
		"-l", "pg.platform.local/run="+run, "--timeout=90s")
	Expect(err).NotTo(HaveOccurred())
}

func checkerOutcome(run string) checkerResult {
	GinkgoHelper()
	_, err := utils.Kubectl("-n", namespace, "wait", "--for=condition=Complete", "job/checker-"+run, "--timeout=6m")
	logs, _ := utils.Kubectl("-n", namespace, "logs", "job/checker-"+run, "--tail=1")
	Expect(err).NotTo(HaveOccurred(), "checker did not pass:\n%s", logs)
	var r checkerResult
	lines := utils.GetNonEmptyLines(logs)
	Expect(lines).NotTo(BeEmpty())
	Expect(json.Unmarshal([]byte(lines[len(lines)-1]), &r)).To(Succeed())
	_, _ = fmt.Fprintf(GinkgoWriter, "checker %s: %s\n", run, lines[len(lines)-1])
	return r
}

var _ = Describe("PostgresCluster", Ordered, func() {
	AfterAll(func() {
		_, _ = utils.Kubectl("-n", namespace, "delete", "pgc", cluster, "--ignore-not-found", "--wait=false")
	})

	It("bootstraps three instances with a synchronous quorum", func() {
		_, err := utils.KubectlStdin(clusterManifest("1", "1Gi"), "apply", "-f", "-")
		Expect(err).NotTo(HaveOccurred())
		Eventually(healthy).Should(BeTrue())

		primary := status("currentPrimary")
		Eventually(func() (string, error) {
			return psql(primary, "",
				"SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming' AND sync_state = 'quorum'")
		}).Should(Equal("2"))

		for _, svc := range []string{"-rw", "-ro", "-instances"} {
			_, err := utils.Kubectl("-n", namespace, "get", "service", cluster+svc)
			Expect(err).NotTo(HaveOccurred())
		}
		holder, err := utils.Kubectl("-n", namespace, "get", "lease", cluster+"-primary",
			"-o", "jsonpath={.spec.holderIdentity}")
		Expect(err).NotTo(HaveOccurred())
		Expect(holder).To(Equal(primary))
	})

	It("rejects invalid specs at admission", func() {
		out, err := utils.KubectlStdin(clusterManifest("3", "1Gi"), "apply", "-f", "-")
		Expect(err).To(HaveOccurred())
		Expect(out).To(ContainSubstring("synchronous.number must be lower than instances"))

		out, err = utils.KubectlStdin(clusterManifest("1", "512Mi"), "apply", "-f", "-")
		Expect(err).To(HaveOccurred())
		Expect(out).To(ContainSubstring("cannot be reduced"))
	})

	It("routes writes to the primary and reads to the standbys", func() {
		replica := ""
		for n := 1; n <= 3; n++ {
			if pod := fmt.Sprintf("%s-%d", cluster, n); pod != status("currentPrimary") {
				replica = pod
				break
			}
		}
		Expect(psql(replica, service("rw"), "SELECT pg_is_in_recovery()")).To(Equal("f"))
		Expect(psql(replica, service("ro"), "SELECT pg_is_in_recovery()")).To(Equal("t"))
	})

	It("fails over under load without losing an acknowledged write", func() {
		oldPrimary := status("currentPrimary")
		oldEpoch, err := strconv.Atoi(status("epoch"))
		Expect(err).NotTo(HaveOccurred())

		startChecker("failover", 60*time.Second)
		time.Sleep(20 * time.Second)
		By("killing primary " + oldPrimary)
		_, err = utils.Kubectl("-n", namespace, "delete", "pod", oldPrimary, "--grace-period=0", "--force")
		Expect(err).NotTo(HaveOccurred())

		r := checkerOutcome("failover")
		Expect(r.Linearizability).To(Equal("Ok"))
		Expect(r.LostWrites).To(BeZero())
		Expect(r.PhantomWrites).To(BeZero())
		Expect(r.AckedWrites).To(BeNumerically(">", 100))
		Expect(r.Passed).To(BeTrue())

		Eventually(healthy).Should(BeTrue())
		Expect(status("currentPrimary")).NotTo(Equal(oldPrimary))
		epoch, err := strconv.Atoi(status("epoch"))
		Expect(err).NotTo(HaveOccurred())
		Expect(epoch).To(BeNumerically(">", oldEpoch))
		d, err := time.ParseDuration(status("lastFailoverDuration"))
		Expect(err).NotTo(HaveOccurred())
		_, _ = fmt.Fprintf(GinkgoWriter, "failover took %s, widest write gap %.0fms\n", d, r.MaxWriteGapMs)
		Expect(d).To(BeNumerically("<", 30*time.Second))

		By("bringing the old primary back as a streaming standby")
		Eventually(func() (string, error) { return psql(oldPrimary, "", "SELECT pg_is_in_recovery()") }).Should(Equal("t"))
		Eventually(func() (string, error) {
			return psql(status("currentPrimary"), "", "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'")
		}).Should(Equal("2"))
	})

	It("switches over when the primary's node is cordoned", func() {
		oldPrimary := status("currentPrimary")
		node, err := utils.Kubectl("-n", namespace, "get", "pod", oldPrimary, "-o", "jsonpath={.spec.nodeName}")
		Expect(err).NotTo(HaveOccurred())
		_, err = psql(oldPrimary, "", "CREATE TABLE IF NOT EXISTS marker (id int PRIMARY KEY); INSERT INTO marker VALUES (1)")
		Expect(err).NotTo(HaveOccurred())

		By("cordoning " + node)
		_, err = utils.Kubectl("cordon", node)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _, _ = utils.Kubectl("uncordon", node) })

		Eventually(func() string { return status("currentPrimary") }).ShouldNot(Equal(oldPrimary))
		newPrimary := status("currentPrimary")
		Expect(psql(newPrimary, "", "SELECT count(*) FROM marker")).To(Equal("1"))

		// The old primary's volume is bound to the cordoned node, so its
		// replacement pod can only start there once the node is back.
		_, err = utils.Kubectl("uncordon", node)
		Expect(err).NotTo(HaveOccurred())
		Eventually(healthy).Should(BeTrue())
	})
})
