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

package common

const (
	// LabelAnnotationPrefix is the prefix of every labels and annotations added by the controller.
	LabelAnnotationPrefix = "data.alluxio.io/"
	// The format is data.alluxio.io/storage-{runtime_type}-{data_set_name}
	LabelAnnotationStorageCapacityPrefix = LabelAnnotationPrefix + "storage-"
	// The dataset annotation
	LabelAnnotationDataset = LabelAnnotationPrefix + "dataset"
)

const (
	// The name of the finalizer that is installed onto managed etcd resources.
	AlluxioRuntimeResourceFinalizerName = "alluxio-runtime-controller-finalizer"

	AlluxioDatasetResourceFinalizerName = "alluxio-dataset-controller-finalizer"
)

//Reason for Pillar events
const (
	ErrorProcessDatasetReason = "ErrorProcessDataset"

	ErrorProcessRuntimeReason = "ErrorProcessRuntime"
)

const RecommendedKubeConfigPathEnv = "KUBECONFIG"

// Helper type that describes the action to perform on a resource.
type ReconcileAction string

// Status
const (
	NeedsCreate   ReconcileAction = "NeedsCreate"
	NeedsDelete   ReconcileAction = "NeedsDelete"
	NeedsNoop     ReconcileAction = "NeedsNoop"
	NeedsUpdate   ReconcileAction = "NeedsUpdate"
	NeedsLoadData ReconcileAction = "NeedsLoadData"
)

// Runtime for Alluxio
const (
	ALLUXIO_RUNTIME = "alluxio"

	ALLUXIO_NAMESPACE = "alluxio-system"

	ALLUXIO_CHART = "alluxio"

	ALLUXIO_DATA_LOADER_IMAGE_ENV = "AlluxioDataLoaderImage"

	DEFAULT_ALLUXIO_DATA_LOADER_IMAGE = "alluxio/alluxio-data-loader:v2.2.0"

	DefaultDDCRuntime = ALLUXIO_RUNTIME
)

var (
	RUNTIMES []string = []string{ALLUXIO_RUNTIME}

	PercentageOfNodeStorageCapacity float64 = 50

	ReservedNodeStorageCapacity string = "1GiB"

	// MEM or DISK
	CacheStoreType string = "MEM"

	CacheStoragePath string = "/dev/shm"
)
