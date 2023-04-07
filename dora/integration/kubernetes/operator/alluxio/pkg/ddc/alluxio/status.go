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
	data "github.com/Alluxio/alluxio/api/v1alpha1"
	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/utils"
	"k8s.io/client-go/util/retry"

	"context"
	"reflect"
)

// update the status of the engine
func (e *AlluxioEngine) UpdateRuntimeStatus(runtime *data.Runtime) (ready bool, err error) {
	var (
		masterReady, workerReady, fuseReady bool
		masterName                          string = e.Config.Name + "-" + e.Type() + "-master"
		namespace                           string = common.ALLUXIO_NAMESPACE
		workerName                          string = e.Config.Name + "-" + e.Type() + "-worker"
		fuseName                            string = e.Config.Name + "-" + e.Type() + "-fuse"
	)

	runtimeToUpdate := runtime.DeepCopy()
	if reflect.DeepEqual(runtime.Status, runtimeToUpdate.Status) {
		e.Log.V(1).Info("The runtime is equal after deepcopy")
	}

	states, err := e.queryCacheStatus()
	if err != nil {
		return ready, err
	}

	// 0. Update the cache status
	// runtimeToUpdate.Status.CacheStates[data.Cacheable] = states.cacheable
	runtimeToUpdate.Status.CacheStates[data.CacheCapacity] = states.cacheCapacity
	runtimeToUpdate.Status.CacheStates[data.CachedPercentage] = states.cachedPercentage
	runtimeToUpdate.Status.CacheStates[data.NonCacheable] = states.nonCacheable
	runtimeToUpdate.Status.CacheStates[data.Cached] = states.cached
	runtimeToUpdate.Status.CacheStates[data.LowWaterMark] = states.lowWaterMark
	runtimeToUpdate.Status.CacheStates[data.HighWaterMark] = states.highWaterMark

	master, err := e.getMasterStatefulset(masterName, namespace)
	if err != nil {
		return ready, err
	}

	// 1. Master should be ready
	runtimeToUpdate.Status.DesiredMasterNumberScheduled = uint32(master.Status.Replicas)
	runtimeToUpdate.Status.MasterNumberReady = uint32(master.Status.ReadyReplicas)
	if master.Status.Replicas == master.Status.ReadyReplicas {
		runtimeToUpdate.Status.MasterPhase = data.RuntimePhaseReady
		masterReady = true
	} else {
		runtimeToUpdate.Status.MasterPhase = data.RuntimePhaseNotReady
	}

	workers, err := e.getDaemonset(workerName, namespace)
	if err != nil {
		return ready, err
	}
	// 2. Worker should be ready
	// runtimeToUpdate.Status.DesiredWorkerNumberScheduled = uint32(workers.Status.DesiredNumberScheduled)
	runtimeToUpdate.Status.WorkerNumberReady = uint32(workers.Status.NumberReady)
	runtimeToUpdate.Status.WorkerNumberUnavailable = uint32(workers.Status.NumberUnavailable)
	runtimeToUpdate.Status.WorkerNumberAvailable = uint32(workers.Status.NumberAvailable)
	if workers.Status.DesiredNumberScheduled == workers.Status.NumberReady {
		runtimeToUpdate.Status.WorkerPhase = data.RuntimePhaseReady
		// runtimeToUpdate.Status.CacheStates[data.Cacheable] = runtime.Status.CacheStates[data.CacheCapacity]
		workerReady = true
	} else {
		runtimeToUpdate.Status.WorkerPhase = data.RuntimePhaseNotReady
		// cacheCapacity := "0"
		// if runtime.Status.CacheStates[data.CacheCapacity] != "" {
		// 	cacheCapacity = runtime.Status.CacheStates[data.CacheCapacity]
		// }

		// cacheCapacityInBytes, err := units.RAMInBytes(cacheCapacity)
		// if err != nil {
		// 	return ready, err
		// }

		// cacheable := float64(cacheCapacityInBytes / int64(runtime.Status.DesiredWorkerNumberScheduled))
		// runtimeToUpdate.Status.CacheStates[data.Cacheable] = units.BytesSize(cacheable)
	}

	fuses, err := e.getDaemonset(fuseName, namespace)
	if err != nil {
		return ready, err
	}
	// 3. fuse shoulde be ready
	// runtimeToUpdate.Status.DesiredFuseNumberScheduled = uint32(fuses.Status.DesiredNumberScheduled)
	runtimeToUpdate.Status.FuseNumberReady = uint32(fuses.Status.NumberReady)
	runtimeToUpdate.Status.FuseNumberUnavailable = uint32(fuses.Status.NumberUnavailable)
	runtimeToUpdate.Status.FuseNumberAvailable = uint32(fuses.Status.NumberAvailable)
	if fuses.Status.DesiredNumberScheduled == fuses.Status.NumberReady {
		runtimeToUpdate.Status.FusePhase = data.RuntimePhaseReady
		fuseReady = true
	} else {
		runtimeToUpdate.Status.FusePhase = data.RuntimePhaseNotReady
	}

	if masterReady && workerReady && fuseReady {
		ready = true
	}

	if !reflect.DeepEqual(runtime.Status, runtimeToUpdate.Status) {
		err = e.Client.Status().Update(context.TODO(), runtimeToUpdate)
		if err != nil {
			e.Log.Error(err, "Failed to update the runtime")
		}
	} else {
		e.Log.Info("Do nothing because the runtime status is not changed.")
	}

	return
}

// update the status of worker
func (e *AlluxioEngine) UpdateCacheStateOfDataset() (err error) {
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		dataset, err := utils.GetDataset(e.Client, e.Config.Name, e.Config.Namespace)
		if err != nil {
			return err
		}

		runtime, err := utils.GetRuntime(e.Client, e.Config.Name, e.Config.Namespace)
		if err != nil {
			return err
		}

		_, err = e.UpdateRuntimeStatus(runtime)
		if err != nil {
			return err
		}

		datasetToUpdate := dataset.DeepCopy()
		datasetToUpdate.Status.CacheStatus.CacheStates = runtime.Status.CacheStates
		// datasetToUpdate.Status.CacheStatus.CacheStates =

		if !reflect.DeepEqual(dataset.Status, datasetToUpdate.Status) {
			e.Log.V(1).Info("update the cache state of dataset")
			err = e.Client.Status().Update(context.TODO(), datasetToUpdate)
			if err != nil {
				e.Log.Error(err, "Update dataset")
				return err
			}
		} else {
			e.Log.V(1).Info("No need to update the cache state of dataset")
		}

		return nil
	})

	return err
}
