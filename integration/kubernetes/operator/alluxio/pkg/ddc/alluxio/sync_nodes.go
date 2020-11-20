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

package alluxio

import (
	"context"
	"reflect"

	data "github.com/Alluxio/alluxio/api/v1alpha1"
	"github.com/Alluxio/alluxio/pkg/utils"
	units "github.com/docker/go-units"
)

// Sync up the nodes for cache capacity, scale out or scale in or do nothing
func (e *AlluxioEngine) SyncNodes() (changed bool, err error) {
	var (
		nodeChanged   = false
		workerChanged = false
	)

	dataset, err := utils.GetDataset(e.Client, e.Config.Name, e.Config.Namespace)
	if err != nil {
		e.Log.Error(err, "Failed to get dataset")
		return changed, err
	}

	runtime, err := utils.GetRuntime(e.Client, e.Config.Name, e.Config.Namespace)
	if err != nil {
		e.Log.Error(err, "Failed to get runtime")
		return changed, err
	}

	datasetToUpdate := dataset.DeepCopy()

	// 1. Worker changed
	workerChanged, err = e.checkWorkersNumberChanged()
	if err != nil {
		return changed, err
	}
	// force update for now
	// workerChanged = true
	if !workerChanged {
		e.Log.Info("Do nothing because the workers are not changed.")
	} else {
		_, err = e.UpdateRuntimeStatus(runtime)
		if err != nil {
			return
		}

		runtime, err = utils.GetRuntime(e.Client, e.Config.Name, e.Config.Namespace)
		if err != nil {
			e.Log.Error(err, "Failed to sync the cache")
			return changed, err
		}

		datasetToUpdate.Status.CacheStatus.CacheStates = runtime.Status.CacheStates
		// datasetToUpdate.Status.CacheStatus.CacheStates =

		if !reflect.DeepEqual(dataset.Status, datasetToUpdate.Status) {
			err = e.Client.Status().Update(context.TODO(), datasetToUpdate)
			if err != nil {
				e.Log.Error(err, "Update dataset")
				return changed, err
			}
			dataset = datasetToUpdate
		}
	}

	// 2.(TODO) check if there is new node added. How to check new node is added
	nonCacheableStr := "0"
	if dataset.Status.CacheStatus.CacheStates[data.NonCacheable] != "" {
		nonCacheableStr = dataset.Status.CacheStatus.CacheStates[data.NonCacheable]
	}

	nonCacheable, err := units.RAMInBytes(nonCacheableStr)
	if err != nil {
		return changed, err
	}

	if nonCacheable > 0 {
		// need a way to check new node is added
		clusterCapacity, err := e.GetNewAvailableForCacheInCluster()
		if err != nil {
			return changed, err
		}

		nodeChanged = clusterCapacity > 0

		if nodeChanged {

			desiredNumber, err := e.SetupWorkers(dataset)
			if err != nil {
				e.Log.Error(err, "Failed to sync the cache")
				return changed, err
			}

			e.Log.Info("SyncNodes", "desiredNumber", desiredNumber)

			// setup runtime desired workers
			runtime, err := utils.GetRuntime(e.Client, e.Config.Name, e.Config.Namespace)
			if err != nil {
				e.Log.Error(err, "Failed to get the runtime")
				return changed, err
			}

			runtimeToUpdate := runtime.DeepCopy()
			runtimeToUpdate.Status.DesiredWorkerNumberScheduled = desiredNumber
			if !reflect.DeepEqual(runtime.Status, runtimeToUpdate.Status) {
				err = e.Client.Status().Update(context.TODO(), runtimeToUpdate)
				if err != nil {
					e.Log.Error(err, "Failed to update the runtime")
					return changed, err
				}
			} else {
				e.Log.Info("Do nothing because the runtime status is not changed.")
			}
		}
	}

	if nodeChanged {
		runtime, err := utils.GetRuntime(e.Client, e.Config.Name, e.Config.Namespace)
		if err != nil {
			e.Log.Error(err, "Failed to sync the cache")
			return changed, err
		}

		ready, err := e.UpdateRuntimeStatus(runtime)
		if err != nil {
			e.Log.Error(err, "Failed to sync the cache")
			return changed, err
		}

		e.Log.Info("check the status of runtime ", "ready", ready)

	}

	changed = nodeChanged && workerChanged

	return changed, nil
}
