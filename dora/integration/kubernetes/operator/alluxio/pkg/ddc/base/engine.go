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

package base

import (
	data "github.com/Alluxio/alluxio/api/v1alpha1"
	"github.com/Alluxio/alluxio/pkg/common"
)

// Engine interface defines the interfaces that should be implemented
// by a distributed data caching Engine.
// Thread safety is required from implementations of this interface.
type Engine interface {
	// Sync the engine
	Setup(req common.ReconcileRequestContext) (bool, error)

	UnderFileSystemService
	// Returns the ID of the ddc Engine
	ID() string

	// Type returns the type of the ddc Engine.
	Type() string

	// Schedule the Kubernetes nodes to run the cache system
	// 1. Order the nodes by memory capacity, from the largest
	AssignNodesToCache(datasetUFSTotalBytes uint64) (desiredNum uint32, err error)
	// update the status of runtime
	UpdateRuntimeStatus(runtime *data.Runtime) (ready bool, err error)
	// update the status of worker
	UpdateCacheStateOfDataset() (err error)
	// Preload the data in the Cache System
	Preload(dataset *data.Dataset) error

	// Check the health status of runtime
	HealthyCheck() (err error)

	// Sync up the nodes for cache capacity, scale out or scale in or do nothing
	SyncNodes() (changed bool, err error)

	// Sync with the UFS
	SyncUFS() (changed bool, err error)

	// Sync the Dataset replicas
	SyncReplicas(replicas *int32) (err error)

	// Load the data
	LoadData() error

	Destroy() error
}

// The real engine should implement
type Implement interface {

	// Type returns the type of the ddc Engine.
	Type() string

	// Is the master ready
	IsMasterReady(runtime *data.Runtime) (ready bool, err error)

	// are the workers ready
	AreWorkersReady(runtime *data.Runtime) (ready bool, err error)

	// setup the cache master
	SetupMaster() (desiredNum uint32, err error)
	// setup the cache worker
	SetupWorkers(dataset *data.Dataset) (desiredNum uint32, err error)
	// update the status of runtime
	UpdateRuntimeStatus(runtime *data.Runtime) (ready bool, err error)
	// update the status of worker
	// UpdateCacheStateOfDataset() (err error)
	// Prepare the mounts and metadata
	PrepareUFS() (err error)
	// Set the Runtime Max Memory
	SetRuntimeMaxMemory(nodeName string, humanReadableMax string) (err error)
	// Get the current cached capacity and the workers number
	GetCurrentCachedCapacity() (totalCapacity uint64, workerNum uint32, err error)

	// Preload the data in the Cache System
	Preload(dataset *data.Dataset) error

	UnderFileSystemService

	// Remove the Cache Nodes
	RemoveCacheNodes() (err error)
	// Destroy the master
	DestroyMaster() error
	// Destroy the workers
	DestroyWorkers() error
	// clean up the cache
	CleanupCache() error
	// clean all
	// clean up the cache
	CleanAll() error
	// The low water mark ratio
	LowWaterMarkRatio() float64
	// The high water mark ratio
	HighWaterMarkRatio() float64
}

// UnderFileSystemService interface defines the interfaces that should be implemented
// by a underlayer fileSystem service for the data. The implementation is the underlayer file system connector.
// It is responsible for checking ufs and preload the data.
// Thread safety is required from implementations of this interface.
type UnderFileSystemService interface {
	UsedStorageBytes() (uint64, error)

	FreeStorageBytes() (uint64, error)

	TotalStorageBytes() (uint64, error)

	TotalFileNums() (uint64, error)
}
