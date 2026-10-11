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
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	failoversTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pg_operator_failovers_total",
		Help: "Failovers and switchovers started, by reason.",
	}, []string{"namespace", "cluster", "reason"})

	failoverSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pg_operator_failover_duration_seconds",
		Help:    "Time from leaving Healthy to Healthy again, as seen by the operator.",
		Buckets: []float64{0.25, 0.5, 1, 2, 3, 5, 8, 13, 21, 34, 55},
	}, []string{"namespace", "cluster", "reason"})
)

func init() {
	metrics.Registry.MustRegister(failoversTotal, failoverSeconds)
}
