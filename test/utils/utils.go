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

// Package utils holds the small helpers the e2e suite uses to drive
// kubectl against the cluster under test.
package utils

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
)

// Run executes cmd from the project root and returns its combined output.
func Run(cmd *exec.Cmd) (string, error) {
	dir, err := GetProjectDir()
	if err != nil {
		return "", err
	}
	cmd.Dir = dir
	command := strings.Join(cmd.Args, " ")
	_, _ = fmt.Fprintf(GinkgoWriter, "running: %s\n", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%s failed: %w\n%s", command, err, output)
	}
	return string(output), nil
}

// Kubectl runs kubectl with the given arguments.
func Kubectl(args ...string) (string, error) {
	return Run(exec.Command("kubectl", args...))
}

// KubectlStdin runs kubectl with the given arguments and stdin.
func KubectlStdin(stdin string, args ...string) (string, error) {
	cmd := exec.Command("kubectl", args...)
	cmd.Stdin = strings.NewReader(stdin)
	return Run(cmd)
}

// GetProjectDir returns the repository root.
func GetProjectDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return wd, fmt.Errorf("failed to get current working directory: %w", err)
	}
	return strings.TrimSuffix(wd, "/test/e2e"), nil
}

// GetNonEmptyLines splits output into its non-empty lines.
func GetNonEmptyLines(output string) []string {
	var res []string
	for line := range strings.SplitSeq(output, "\n") {
		if line != "" {
			res = append(res, line)
		}
	}
	return res
}
