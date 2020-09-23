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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RuntimeType describes the ddc runtime for cache.
// +kubebuilder:validation:Enum=Alluxio;
type RuntimeType string

const (
	// AlluxioRuntimeType will create the cache runtime by using Alluxio
	AlluxioRuntimeType RuntimeType = "Alluxio"
)

// RuntimeSpec defines the desired state of Runtime
type RuntimeSpec struct {
	// type field of Runtime. Now only support Alluxio,
	// to make it flexible, we support string for now
	// +kubebuilder:validation:MinLength=1
	// +required
	Type string `json:"type,omitempty"`

	// The name of the ConfigMap which contains the Value config files.
	// The ConfigMap must be in the same namespace as the Cache Runtime.
	// +required
	// +kubebuilder:validation:MinLength=1
	TemplateFileName string `json:"templateFileName,omitempty"`

	// Runtime options which are appened to configMaps.
	// +optional
	Options map[string]string `json:"options,omitempty"`
}

type RuntimePhase string

const (
	RuntimePhaseNone     RuntimePhase = ""
	RuntimePhaseNotReady RuntimePhase = "NotReady"
	RuntimePhaseReady    RuntimePhase = "Ready"
)

// RuntimeStatus defines the observed state of Runtime
type RuntimeStatus struct {
	ValueFileConfigmap string `json:"valueFile"`
	// MasterPhase is the master running phase
	MasterPhase  RuntimePhase `json:"masterPhase"`
	MasterReason string       `json:"masterReason,omitempty"`

	// WorkerPhase is the worker running phase
	WorkerPhase  RuntimePhase `json:"workerPhase"`
	WorkerReason string       `json:"workerReason,omitempty"`

	// The total number of nodes that should be running the runtime worker
	// pod (including nodes correctly running the runtime worker pod).
	DesiredWorkerNumberScheduled uint32 `json:"desiredWorkerNumberScheduled"`

	// The number of nodes that should be running the runtime worker pod and have one
	// or more of the runtime worker pod running and ready.
	WorkerNumberReady uint32 `json:"workerNumberReady"`

	// The number of nodes that should be running the
	// runtime worker pod and have one or more of the runtime worker pod running and
	// available (ready for at least spec.minReadySeconds)
	// +optional
	WorkerNumberAvailable uint32 `json:"workerNumberAvailable,omitempty"`

	// The number of nodes that should be running the
	// runtime worker pod and have none of the runtime worker pod running and available
	// (ready for at least spec.minReadySeconds)
	// +optional
	WorkerNumberUnavailable uint32 `json:"workerNumberUnavailable,omitempty"`

	// The total number of nodes that should be running the runtime
	// pod (including nodes correctly running the runtime master pod).
	DesiredMasterNumberScheduled uint32 `json:"desiredMasterNumberScheduled"`

	// The number of nodes that should be running the runtime worker pod and have zero
	// or more of the runtime master pod running and ready.
	MasterNumberReady uint32 `json:"masterNumberReady"`

	// FusePhase is the Fuse running phase
	FusePhase  RuntimePhase `json:"fusePhase"`
	FuseReason string       `json:"fuseReason,omitempty"`

	// The total number of nodes that should be running the runtime Fuse
	// pod (including nodes correctly running the runtime Fuse pod).
	DesiredFuseNumberScheduled uint32 `json:"desiredFuseNumberScheduled"`

	// The number of nodes that should be running the runtime Fuse pod and have one
	// or more of the runtime Fuse pod running and ready.
	FuseNumberReady uint32 `json:"fuseNumberReady"`

	// The number of nodes that should be running the
	// runtime fuse pod and have none of the runtime fuse pod running and available
	// (ready for at least spec.minReadySeconds)
	// +optional
	FuseNumberUnavailable uint32 `json:"fuseNumberUnavailable,omitempty"`

	// The number of nodes that should be running the
	// runtime Fuse pod and have one or more of the runtime Fuse pod running and
	// available (ready for at least spec.minReadySeconds)
	// +optional
	FuseNumberAvailable uint32 `json:"fuseNumberAvailable,omitempty"`

	// Represents the latest available observations of a ddc runtime's current state.
	// +patchMergeKey=type
	// +patchStrategy=merge
	Conditions []RuntimeCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// CacheStatus represents the total resources of the dataset.
	CacheStates CacheStateList `json:"cacheStates,omitempty"`
}

// RuntimeConditionType indicates valid conditions type of a runtime
type RuntimeConditionType string

// These are valid conditions of a runtime.
const (
	// MasterInitialized means the master of runtime is initialized
	RuntimeMasterInitialized RuntimeConditionType = "MasterInitialized"
	// MasterReady means the master of runtime is ready
	RuntimeMasterReady RuntimeConditionType = "MasterReady"
	// WorkersInitialized means the Workers of runtime is initialized
	RuntimeWorkersInitialized RuntimeConditionType = "WorkersInitialized"
	// WorkersReady means the Workers of runtime is ready
	RuntimeWorkersReady RuntimeConditionType = "WorkersReady"
	// FusesInitialized means the fuses of runtime is initialized
	RuntimeFusesInitialized RuntimeConditionType = "FusesInitialized"
	// FusesReady means the fuses of runtime is ready
	RuntimeFusesReady RuntimeConditionType = "FusesReady"
	// Loading means the runtime is Loading data
	RuntimeDataLoading RuntimeConditionType = "DataLoading"
	// Loaded means the data of runtime is Loaded.
	RuntimeDataLoaded RuntimeConditionType = "DataLoaded"
)

const (
	RuntimeMasterInitializedReason = "Master is initialized"
	// MasterReady means the master of runtime is ready
	RuntimeMasterReadyReason = "Master is ready"
	// WorkersInitialized means the Workers of runtime is initialized
	RuntimeWorkersInitializedReason = "Workers are initialized"
	// WorkersReady means the Workers of runtime is ready
	RuntimeWorkersReadyReason = "Workers are ready"
	// WorkersInitialized means the Workers of runtime is initialized
	RuntimeFusesInitializedReason = "Fuses are initialized"
	// WorkersReady means the Workers of runtime is ready
	RuntimeFusesReadyReason = "Fuses are ready"
	// Loading means the runtime is Loading data
	RuntimeDataLoadingReason = "Data are loading"
	// Loaded means the data of runtime is Loaded.
	RuntimeDataLoadedReason = "Data are loaded"
)

// Condition describes the state of the cache at a certain point.
type RuntimeCondition struct {
	// Type of cache condition.
	Type RuntimeConditionType `json:"type"`
	// Status of the condition, one of True, False, Unknown.
	Status v1.ConditionStatus `json:"status"`
	// The reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`
	// A human readable message indicating details about the transition.
	Message string `json:"message,omitempty"`
	// The last time this condition was updated.
	LastProbeTime metav1.Time `json:"lastProbeTime,omitempty"`
	// Last time the condition transitioned from one status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Runtime is the Schema for the runtimes API
type Runtime struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RuntimeSpec   `json:"spec,omitempty"`
	Status RuntimeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RuntimeList contains a list of Runtime
type RuntimeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Runtime `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Runtime{}, &RuntimeList{})
}
