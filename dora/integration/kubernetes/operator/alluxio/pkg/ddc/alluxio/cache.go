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
	"fmt"

	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/ddc/alluxio/operations"
	"github.com/Alluxio/alluxio/pkg/utils"
	units "github.com/docker/go-units"
)

// // sync runtime cache status
// func (e *AlluxioEngine) syncRuntimeCacheStatus() (err error) {
// 	cacheCapacity, err := e.getCurrentCachedCapacity()
// 	if err != nil {
// 		e.Log.Error(err, "Failed to sync the cache")
// 		return err
// 	}

// 	dataset, err := utils.GetDataset(e.Client, e.Config.Name, e.Config.Namespace)
// 	if err != nil {
// 		e.Log.Error(err, "Failed to sync the cache")
// 		return err
// 	}

// 	// The overall size of the dataset need to be cached
// 	totalToCacheStr := "0"

// 	if dataset.Status.Total != "" {
// 		totalToCacheStr = dataset.Status.Total
// 	}

// 	totalToCache, err := units.RAMInBytes(totalToCacheStr)
// 	if err != nil {
// 		return err
// 	}

// 	// (todo) cheyang: cached is zero for now, it can be got by using operation for future
// 	nonCacheable := totalToCache - cacheCapacity

// 	runtime, err := utils.GetRuntime(e.Client, e.Config.Name, e.Config.Namespace)
// 	if err != nil {
// 		return err
// 	}

// 	runtimeToUpdate := runtime.DeepCopy()

// 	runtimeToUpdate.Status.CacheStates[data.CacheCapacity] = units.BytesSize(float64(cacheCapacity))
// 	// (todo) cheyang: cached is zero for now, it can be got by using operation for future
// 	runtimeToUpdate.Status.CacheStates[data.Cacheable] = runtimeToUpdate.Status.CacheStates[data.CacheCapacity]
// 	runtimeToUpdate.Status.CacheStates[data.NonCacheable] = units.BytesSize(float64(nonCacheable))

// 	if !reflect.DeepEqual(runtime.Status, runtimeToUpdate.Status) {

// 		err = e.Client.Status().Update(context.TODO(), runtimeToUpdate)
// 		if err != nil {
// 			e.Log.Error(err, "Failed to syncCacheStatus")
// 			return err
// 		}
// 	}

// 	return nil
// }

// queryCacheStatus checks the cache status
func (e *AlluxioEngine) queryCacheStatus() (states cacheStates, err error) {
	cacheCapacity, _, err := e.GetCurrentCachedCapacity()
	if err != nil {
		e.Log.Error(err, "Failed to sync the cache")
		return states, err
	}

	dataset, err := utils.GetDataset(e.Client, e.Config.Name, e.Config.Namespace)
	if err != nil {
		e.Log.Error(err, "Failed to sync the cache")
		return states, err
	}

	// The overall size of the dataset need to be cached
	totalToCacheStr := "0"

	if dataset.Status.Total != "" {
		totalToCacheStr = dataset.Status.Total
	}

	totalToCache, err := units.RAMInBytes(totalToCacheStr)
	if err != nil {
		return states, err
	}

	// (todo) cheyang: cached is zero for now, it can be got by using operation for future
	// nonCacheable := uint64(totalToCache) - cacheCapacity
	var nonCacheable uint64 = 0
	if uint64(totalToCache) > uint64(float64(cacheCapacity)*e.Config.LowWaterMarkRatio) {
		nonCacheable = uint64(totalToCache) - uint64(float64(cacheCapacity)*e.Config.LowWaterMarkRatio)
	}

	// check the cached
	cached, err := e.cachedState()
	if err != nil {
		return states, err
	}
	cachedPercentage := utils.PercentOfFloat(float64(cached), float64(totalToCache))

	if cachedPercentage > 100 {
		cachedPercentage = 100.00
	}

	return cacheStates{
		cacheCapacity: units.BytesSize(float64(cacheCapacity)),
		lowWaterMark:  units.BytesSize(float64(cacheCapacity) * e.Config.LowWaterMarkRatio),
		highWaterMark: units.BytesSize(float64(cacheCapacity) * e.Config.HighWaterMarkRatio),
		// cacheable:        units.BytesSize(float64(cacheCapacity) - float64(cached)),
		nonCacheable:     units.BytesSize(float64(nonCacheable)),
		cached:           units.BytesSize(float64(cached)),
		cachedPercentage: fmt.Sprintf("%.2f %%", cachedPercentage),
	}, nil
}

// get the value of cached
func (e *AlluxioEngine) cachedState() (uint64, error) {
	var (
		podName       = e.Config.Name + "-" + e.Type() + "-master-0"
		containerName = "alluxio-master"
		namespace     = common.ALLUXIO_NAMESPACE
	)

	fileUitls := operations.NewAlluxioFileUtils(podName, containerName, namespace, e.Log)
	cached, err := fileUitls.CachedState()

	return uint64(cached), err

}

// clean cache
func (e *AlluxioEngine) invokeCleanCache(path string) (err error) {
	var (
		podName       = e.Config.Name + "-" + e.Type() + "-master-0"
		containerName = "alluxio-master"
		namespace     = common.ALLUXIO_NAMESPACE
	)

	fileUitls := operations.NewAlluxioFileUtils(podName, containerName, namespace, e.Log)
	return fileUitls.CleanCache(path)

}
