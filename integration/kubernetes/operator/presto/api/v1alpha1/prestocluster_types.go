/*
Copyright 2022.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// PrestoClusterSpec defines the desired state of PrestoCluster
type PrestoClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// WorkerNum is the number of presto workers in the presto cluster.
	WorkerNum int32 `json:"size"`
}

// PrestoClusterStatus defines the observed state of PrestoCluster
type PrestoClusterStatus struct {
	// Nodes is a list of the status of the presto workers
	Nodes []string `json:"nodes"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// PrestoCluster is the Schema for the prestoclusters API
type PrestoCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PrestoClusterSpec   `json:"spec,omitempty"`
	Status PrestoClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// PrestoClusterList contains a list of PrestoCluster
type PrestoClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PrestoCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PrestoCluster{}, &PrestoClusterList{})
}
