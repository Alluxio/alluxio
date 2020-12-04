/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package v1alpha1

import (
	"k8s.io/api/core/v1"
	// "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

/**
* apiVersion: "data-orchestration.org/v1aphla1"
* kind: "Dataset"
* metadata:
*   name: "imagenet"
* spec:
*   # target: /imagenet # optional
*   mountPoint: oss://imagenet-huabei5/images/
*   options:
*      fs.oss.endpoint: oss-cn-huhehaote-internal.aliyuncs.com
*      fs.oss.accessKeyId: xxx
*      fs.oss.accessKeySecret: yyy
*   minReplicas: 1 # optional
*   maxReplicas: 3
*   affinity:
*     nodeAffinity:
*       preferredDuringSchedulingIgnoredDuringExecution:
*       - weight: 1
*         preference:
*           matchExpressions:
*           - key: another-node-label-key
*             operator: Exists
* status:
*   total: 50GiB
*   ufsTotal: 50GiB
*   cacheStatus:
*       cached: 15GiB
*       cachable: 40GiB
*       needMoreForCache: 10GiB
 */

// Mount describes a mounting.
type Mount struct {
	// MountPoint is the mount point of source
	// +kubebuilder:validation:MinLength=10
	// +required
	MountPoint string `json:"mountPoint,omitempty"`

	// Options to configure
	// +optional
	Options map[string]string `json:"options,omitempty"`

	// The name of mount
	// +kubebuilder:validation:MinLength=0
	// +required
	Name string `json:"name,omitempty"`

	// Optional: Defaults to false (read-write).
	// +optional
	ReadOnly bool `json:"readOnly,omitempty"`

	// Optional: Defaults to false (shared).
	// +optional
	Shared bool `json:"shared,omitempty"`
}

// DatasetSpec defines the desired state of Dataset
type DatasetSpec struct {
	// Mounts
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:UniqueItems=false
	// +required
	Mounts []Mount `json:"mounts,omitempty"`

	// Specifies how to prefetch dataset.
	// Valid values are:
	// - "Never" (default): never prefetch the dataset;
	// - "Always": always prefetch the dataset;
	// - "OnDemand": will prefetch when application which consumes the dataset is running
	// +optional
	PrefetchStrategy PrefetchStrategy `json:"prefetchStrategy,omitempty"`

	// +kubebuilder:validation:Minimum=1
	// replicas is the min replicas of dataset in the cluster
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	//floats are not support https://github.com/kubernetes-sigs/controller-tools/blob/abcf7d8d54b0cca0f42c470b705fd3cfe768ede3/pkg/crd/schema.go#L387
	// lowWaterMarkRatio is the
	// +optional
	// LowWaterMarkRatio resource.Quantity `json:"lowWaterMarkRatio,omitempty"`
	LowWaterMarkRatio string `json:"lowWaterMarkRatio,omitempty"`

	//floats are not support https://github.com/kubernetes-sigs/controller-tools/blob/abcf7d8d54b0cca0f42c470b705fd3cfe768ede3/pkg/crd/schema.go#L387
	// highWaterMarkRatio is the
	// +optional
	HighWaterMarkRatio string `json:"highWaterMarkRatio,omitempty"`

	// NodeAffinity defines constraints that limit what nodes this dataset can be cached to.
	// This field influences the scheduling of pods that use the cached dataset.
	// +optional
	NodeAffinity *CacheableNodeAffinity `json:"nodeAffinity,omitempty"`

	// If specified, the cache's tolerations.
	// +optional
	Tolerations []v1.Toleration `json:"tolerations,omitempty"`

	// The runtime handle the dataset, for now, only support alluxio.
	// +optional
	Runtime string `json:"runtime,omitempty"`
}

// PrefetchStrategy describes how the dataset will be prefetched for cache.
// Only one of the following Prefetch policies may be specified.
// If none of the following policies is specified, the default one
// is NeverPrefetch.
// +kubebuilder:validation:Enum=Never;Always;OnDemand
type PrefetchStrategy string

const (
	// AlwaysPrefetch will always prefetch.
	AlwaysPrefetch PrefetchStrategy = "Always"

	// OnDemandPrefetch will prefetch when application which consumes the dataset
	// is running.
	OnDemandPrefetch PrefetchStrategy = "OnDemand"

	// NeverPrefetch cancels currently running job and replaces it with a new one.
	NeverPrefetch PrefetchStrategy = "Never"
)

// CacheableNodeAffinity defines constraints that limit what nodes this dataset can be cached to.
type CacheableNodeAffinity struct {
	// Required specifies hard node constraints that must be met.
	Required *v1.NodeSelector `json:"required,omitempty"`
}

// CacheStateName is the name identifying various cacheStateName in a CacheStateNameList.
type CacheStateName string

// ResourceList is a set of (resource name, quantity) pairs.
type CacheStateList map[CacheStateName]string

// CacheStateName names must be not more than 63 characters, consisting of upper- or lower-case alphanumeric characters,
// with the -, _, and . characters allowed anywhere, except the first or last character.
// The default convention, matching that for annotations, is to use lower-case names, with dashes, rather than
// camel case, separating compound words.
// Fully-qualified resource typenames are constructed from a DNS-style subdomain, followed by a slash `/` and a name.
const (
	// Cached in bytes. (500Gi = 500GiB = 500 * 1024 * 1024 * 1024)
	Cached CacheStateName = "cached"
	// Memory, in bytes. (500Gi = 500GiB = 500 * 1024 * 1024 * 1024)
	// Cacheable CacheStateName = "cacheable"
	LowWaterMark CacheStateName = "lowWaterMark"
	// Memory, in bytes. (500Gi = 500GiB = 500 * 1024 * 1024 * 1024)
	HighWaterMark CacheStateName = "highWaterMark"
	// NonCacheable size, in bytes (e,g. 5Gi = 5GiB = 5 * 1024 * 1024 * 1024)
	NonCacheable CacheStateName = "nonCacheable"
	// Percentage represents the cache percentage over the total data in the underlayer filesystem.
	// 1.5 = 1500m
	CachedPercentage CacheStateName = "cachedPercentage"

	CacheCapacity CacheStateName = "cacheCapacity"
)

// The dataset phase indicates whether the loading is behaving
type DatasetPhase string

const (
	// planning the cache
	// Planning Phase = "planning"
	// Loading the cache
	PendingDatasetPhase DatasetPhase = "Pending"
	// loaded the cache
	ReadyDatasetPhase DatasetPhase = "Ready"
)

// Condition describes the state of the cache at a certain point.
type CacheCondition struct {
	// Type of cache condition.
	Type CacheConditionType `json:"type"`
	// Status of the condition, one of True, False, Unknown.
	Status v1.ConditionStatus `json:"status"`
	// The reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`
	// A human readable message indicating details about the transition.
	Message string `json:"message,omitempty"`
	// The last time this condition was updated.
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
	// Last time the condition transitioned from one status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
}

// CacheConditionType defines all kinds of types of cacheStatus.
type CacheConditionType string

const (
	// Planned means the CRD has been accepted by the system,
	// the master and one of the worker has been started,
	// but the data has not been loaded into the distributed cache system.
	Planned CacheConditionType = "Planned"

	// Preloading means the cache system for the dataset has been Preloading.
	Preloading CacheConditionType = "Preloading"

	// Preloading means the cache system for the dataset has been Preloading.
	Preloaded CacheConditionType = "Preloaded"

	// The cache system are ready
	Ready CacheConditionType = "Ready"

	// Resynced means updating with the underlayer filesystem.
	Resynced CacheConditionType = "Resynced"

	// Resynced means updating with the underlayer filesystem.
	DataSetFailed CacheConditionType = "Failed"
)

const (
	// Planned means the CRD has been accepted by the system,
	// the master and one of the worker has been started,
	// but the data has not been loaded into the distributed cache system.
	DatasetPlannedReason = "DatasetPlanned"

	// PreLoading means the cache system for the dataset has been PreLoading.
	DatasetPreLoadingReason = "DatasetPreLoading"

	// PreLoading means the cache system for the dataset has been PreLoading.
	DatasetPreLoadedReason = "DatasetPreLoaded"

	// The cache system are ready
	DatasetReadyReason = "DatasetReady"

	// Resynced means updating with the underlayer filesystem.
	DatasetResyncedReason = "DatasetResynced"

	// Resynced means updating with the underlayer filesystem.
	DatasetDataSetFailedReason = "DatasetFailed"
)

type CacheStatus struct {
	// Conditions is an array of current observed cache conditions.
	Conditions []CacheCondition `json:"conditions"`

	// CacheStatus represents the total resources of the dataset.
	CacheStates CacheStateList `json:"cacheStates,omitempty"`
}

// DatasetStatus defines the observed state of Dataset
type DatasetStatus struct {
	// Dataset Phase
	Phase DatasetPhase `json:"phase,omitempty"`

	// // Minial Total in bytes of the dataset to load in the cluster
	// MinTotal string `json:"minTotal,omitempty"`

	// Max Total in GB of the dataset to load in the cluster
	Total string `json:"total,omitempty"`

	// Total in GB of dataset in the cluster
	UfsTotal string `json:"ufsTotal,omitempty"`

	// CacheStatus represents the total resources of the dataset.
	CacheStatus CacheStatus `json:"cacheStatus,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Dataset is the Schema for the datasets API
type Dataset struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DatasetSpec   `json:"spec,omitempty"`
	Status DatasetStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// DatasetList contains a list of Dataset
type DatasetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Dataset `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Dataset{}, &DatasetList{})
}

// +kubebuilder:docs-gen:collapse=Root Object Definitions
