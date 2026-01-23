/*
Copyright 2016 The Kubernetes Authors All rights reserved.

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

package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	basemetrics "k8s.io/component-base/metrics"

	"k8s.io/kube-state-metrics/v2/pkg/metric"
	generator "k8s.io/kube-state-metrics/v2/pkg/metric_generator"
)

var (
	descCronJobAnnotationsName     = "kube_cronjob_annotations" //nolint:gosec
	descCronJobAnnotationsHelp     = "Kubernetes annotations converted to Prometheus labels."
	descCronJobLabelsName          = "kube_cronjob_labels"
	descCronJobLabelsHelp          = "Kubernetes labels converted to Prometheus labels."
	descCronJobLabelsDefaultLabels = []string{"namespace", "cronjob"}
)

// cronJobWrapper provides a unified interface for both v1 and v1beta1 CronJob objects.
type cronJobWrapper struct {
	Name                       string
	Namespace                  string
	Labels                     map[string]string
	Annotations                map[string]string
	CreationTimestamp          metav1.Time
	ResourceVersion            string
	Schedule                   string
	ConcurrencyPolicy          string
	Suspend                    *bool
	StartingDeadlineSeconds    *int64
	SuccessfulJobsHistoryLimit *int32
	FailedJobsHistoryLimit     *int32
	TimeZone                   *string
	Active                     []v1.ObjectReference
	LastScheduleTime           *metav1.Time
	LastSuccessfulTime         *metav1.Time
}

func cronJobWrapperFromV1(cj *batchv1.CronJob) *cronJobWrapper {
	return &cronJobWrapper{
		Name:                       cj.Name,
		Namespace:                  cj.Namespace,
		Labels:                     cj.Labels,
		Annotations:                cj.Annotations,
		CreationTimestamp:          cj.CreationTimestamp,
		ResourceVersion:            cj.ResourceVersion,
		Schedule:                   cj.Spec.Schedule,
		ConcurrencyPolicy:          string(cj.Spec.ConcurrencyPolicy),
		Suspend:                    cj.Spec.Suspend,
		StartingDeadlineSeconds:    cj.Spec.StartingDeadlineSeconds,
		SuccessfulJobsHistoryLimit: cj.Spec.SuccessfulJobsHistoryLimit,
		FailedJobsHistoryLimit:     cj.Spec.FailedJobsHistoryLimit,
		TimeZone:                   cj.Spec.TimeZone,
		Active:                     cj.Status.Active,
		LastScheduleTime:           cj.Status.LastScheduleTime,
		LastSuccessfulTime:         cj.Status.LastSuccessfulTime,
	}
}

func cronJobWrapperFromV1beta1(cj *batchv1beta1.CronJob) *cronJobWrapper {
	return &cronJobWrapper{
		Name:                       cj.Name,
		Namespace:                  cj.Namespace,
		Labels:                     cj.Labels,
		Annotations:                cj.Annotations,
		CreationTimestamp:          cj.CreationTimestamp,
		ResourceVersion:            cj.ResourceVersion,
		Schedule:                   cj.Spec.Schedule,
		ConcurrencyPolicy:          string(cj.Spec.ConcurrencyPolicy),
		Suspend:                    cj.Spec.Suspend,
		StartingDeadlineSeconds:    cj.Spec.StartingDeadlineSeconds,
		SuccessfulJobsHistoryLimit: cj.Spec.SuccessfulJobsHistoryLimit,
		FailedJobsHistoryLimit:     cj.Spec.FailedJobsHistoryLimit,
		Active:                     cj.Status.Active,
		LastScheduleTime:           cj.Status.LastScheduleTime,
	}
}

func cronJobMetricFamilies(allowAnnotationsList, allowLabelsList []string) []generator.FamilyGenerator {
	return []generator.FamilyGenerator{
		*generator.NewFamilyGeneratorWithStability(
			descCronJobAnnotationsName,
			descCronJobAnnotationsHelp,
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapCronJobFunc(func(j *cronJobWrapper) *metric.Family {
				if len(allowAnnotationsList) == 0 {
					return &metric.Family{}
				}
				annotationKeys, annotationValues := createPrometheusLabelKeysValues("annotation", j.Annotations, allowAnnotationsList)
				return &metric.Family{
					Metrics: []*metric.Metric{
						{
							LabelKeys:   annotationKeys,
							LabelValues: annotationValues,
							Value:       1,
						},
					},
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			descCronJobLabelsName,
			descCronJobLabelsHelp,
			metric.Gauge,
			basemetrics.STABLE,
			"",
			wrapCronJobFunc(func(j *cronJobWrapper) *metric.Family {
				if len(allowLabelsList) == 0 {
					return &metric.Family{}
				}
				labelKeys, labelValues := createPrometheusLabelKeysValues("label", j.Labels, allowLabelsList)
				return &metric.Family{
					Metrics: []*metric.Metric{
						{
							LabelKeys:   labelKeys,
							LabelValues: labelValues,
							Value:       1,
						},
					},
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_cronjob_info",
			"Info about cronjob.",
			metric.Gauge,
			basemetrics.STABLE,
			"",
			wrapCronJobFunc(func(j *cronJobWrapper) *metric.Family {
				timeZone := "local"
				if j.TimeZone != nil {
					timeZone = *j.TimeZone
				}
				return &metric.Family{
					Metrics: []*metric.Metric{
						{
							LabelKeys:   []string{"schedule", "concurrency_policy", "timezone"},
							LabelValues: []string{j.Schedule, j.ConcurrencyPolicy, timeZone},
							Value:       1,
						},
					},
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_cronjob_created",
			"Unix creation timestamp",
			metric.Gauge,
			basemetrics.STABLE,
			"",
			wrapCronJobFunc(func(j *cronJobWrapper) *metric.Family {
				ms := []*metric.Metric{}
				if !j.CreationTimestamp.IsZero() {
					ms = append(ms, &metric.Metric{
						LabelKeys:   []string{},
						LabelValues: []string{},
						Value:       float64(j.CreationTimestamp.Unix()),
					})
				}

				return &metric.Family{
					Metrics: ms,
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_cronjob_status_active",
			"Active holds pointers to currently running jobs.",
			metric.Gauge,
			basemetrics.STABLE,
			"",
			wrapCronJobFunc(func(j *cronJobWrapper) *metric.Family {
				return &metric.Family{
					Metrics: []*metric.Metric{
						{
							LabelKeys:   []string{},
							LabelValues: []string{},
							Value:       float64(len(j.Active)),
						},
					},
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_cronjob_status_last_schedule_time",
			"LastScheduleTime keeps information of when was the last time the job was successfully scheduled.",
			metric.Gauge,
			basemetrics.STABLE,
			"",
			wrapCronJobFunc(func(j *cronJobWrapper) *metric.Family {
				ms := []*metric.Metric{}

				if j.LastScheduleTime != nil {
					ms = append(ms, &metric.Metric{
						LabelKeys:   []string{},
						LabelValues: []string{},
						Value:       float64(j.LastScheduleTime.Unix()),
					})
				}

				return &metric.Family{
					Metrics: ms,
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_cronjob_status_last_successful_time",
			"LastSuccessfulTime keeps information of when was the last time the job was completed successfully.",
			metric.Gauge,
			basemetrics.STABLE,
			"",
			wrapCronJobFunc(func(j *cronJobWrapper) *metric.Family {
				ms := []*metric.Metric{}

				if j.LastSuccessfulTime != nil {
					ms = append(ms, &metric.Metric{
						LabelKeys:   []string{},
						LabelValues: []string{},
						Value:       float64(j.LastSuccessfulTime.Unix()),
					})
				}

				return &metric.Family{
					Metrics: ms,
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_cronjob_spec_suspend",
			"Suspend flag tells the controller to suspend subsequent executions.",
			metric.Gauge,
			basemetrics.STABLE,
			"",
			wrapCronJobFunc(func(j *cronJobWrapper) *metric.Family {
				ms := []*metric.Metric{}

				if j.Suspend != nil {
					ms = append(ms, &metric.Metric{
						LabelKeys:   []string{},
						LabelValues: []string{},
						Value:       boolFloat64(*j.Suspend),
					})
				}

				return &metric.Family{
					Metrics: ms,
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_cronjob_spec_starting_deadline_seconds",
			"Deadline in seconds for starting the job if it misses scheduled time for any reason.",
			metric.Gauge,
			basemetrics.STABLE,
			"",
			wrapCronJobFunc(func(j *cronJobWrapper) *metric.Family {
				ms := []*metric.Metric{}

				if j.StartingDeadlineSeconds != nil {
					ms = append(ms, &metric.Metric{
						LabelKeys:   []string{},
						LabelValues: []string{},
						Value:       float64(*j.StartingDeadlineSeconds),
					})

				}

				return &metric.Family{
					Metrics: ms,
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_cronjob_next_schedule_time",
			"Next time the cronjob should be scheduled. The time after lastScheduleTime, or after the cron job's creation time if it's never been scheduled. Use this to determine if the job is delayed.",
			metric.Gauge,
			basemetrics.STABLE,
			"",
			wrapCronJobFunc(func(j *cronJobWrapper) *metric.Family {
				ms := []*metric.Metric{}

				// If the cron job is suspended, don't track the next scheduled time
				nextScheduledTime, err := getNextScheduledTime(j.Schedule, j.LastScheduleTime, j.CreationTimestamp, j.TimeZone)
				if err != nil {
					panic(err)
				} else if !*j.Suspend {
					ms = append(ms, &metric.Metric{
						LabelKeys:   []string{},
						LabelValues: []string{},
						Value:       float64(nextScheduledTime.Unix()),
					})
				}

				return &metric.Family{
					Metrics: ms,
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_cronjob_metadata_resource_version",
			"Resource version representing a specific version of the cronjob.",
			metric.Gauge,
			basemetrics.STABLE,
			"",
			wrapCronJobFunc(func(j *cronJobWrapper) *metric.Family {
				return &metric.Family{
					Metrics: resourceVersionMetric(j.ResourceVersion),
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_cronjob_spec_successful_job_history_limit",
			"Successful job history limit tells the controller how many completed jobs should be preserved.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapCronJobFunc(func(j *cronJobWrapper) *metric.Family {
				ms := []*metric.Metric{}

				if j.SuccessfulJobsHistoryLimit != nil {
					ms = append(ms, &metric.Metric{
						LabelKeys:   []string{},
						LabelValues: []string{},
						Value:       float64(*j.SuccessfulJobsHistoryLimit),
					})
				}

				return &metric.Family{
					Metrics: ms,
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_cronjob_spec_failed_job_history_limit",
			"Failed job history limit tells the controller how many failed jobs should be preserved.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapCronJobFunc(func(j *cronJobWrapper) *metric.Family {
				ms := []*metric.Metric{}

				if j.FailedJobsHistoryLimit != nil {
					ms = append(ms, &metric.Metric{
						LabelKeys:   []string{},
						LabelValues: []string{},
						Value:       float64(*j.FailedJobsHistoryLimit),
					})
				}

				return &metric.Family{
					Metrics: ms,
				}
			}),
		),
	}
}

func wrapCronJobFunc(f func(*cronJobWrapper) *metric.Family) func(interface{}) *metric.Family {
	return func(obj interface{}) *metric.Family {
		var wrapper *cronJobWrapper
		switch cj := obj.(type) {
		case *batchv1.CronJob:
			wrapper = cronJobWrapperFromV1(cj)
		case *batchv1beta1.CronJob:
			wrapper = cronJobWrapperFromV1beta1(cj)
		default:
			panic(fmt.Errorf("unexpected type %T", obj))
		}

		metricFamily := f(wrapper)

		for _, m := range metricFamily.Metrics {
			m.LabelKeys, m.LabelValues = mergeKeyValues(descCronJobLabelsDefaultLabels, []string{wrapper.Namespace, wrapper.Name}, m.LabelKeys, m.LabelValues)
		}

		return metricFamily
	}
}

func createCronJobListWatch(kubeClient clientset.Interface, ns string, fieldSelector string) cache.ListerWatcher {
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			opts.FieldSelector = fieldSelector
			return kubeClient.BatchV1().CronJobs(ns).List(context.TODO(), opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			opts.FieldSelector = fieldSelector
			return kubeClient.BatchV1().CronJobs(ns).Watch(context.TODO(), opts)
		},
	}
}

func createCronJobListWatchV1beta1(kubeClient clientset.Interface, ns string, fieldSelector string) cache.ListerWatcher {
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			opts.FieldSelector = fieldSelector
			return kubeClient.BatchV1beta1().CronJobs(ns).List(context.TODO(), opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			opts.FieldSelector = fieldSelector
			return kubeClient.BatchV1beta1().CronJobs(ns).Watch(context.TODO(), opts)
		},
	}
}

func getNextScheduledTime(schedule string, lastScheduleTime *metav1.Time, createdTime metav1.Time, timeZone *string) (time.Time, error) {
	if timeZone != nil {
		schedule = fmt.Sprintf("CRON_TZ=%s %s", *timeZone, schedule)
	}

	sched, err := cron.ParseStandard(schedule)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse cron job schedule '%s': %w", schedule, err)
	}
	if !lastScheduleTime.IsZero() {
		return sched.Next(lastScheduleTime.Time), nil
	}
	if !createdTime.IsZero() {
		return sched.Next(createdTime.Time), nil
	}
	return time.Time{}, errors.New("createdTime and lastScheduleTime are both zero")
}
