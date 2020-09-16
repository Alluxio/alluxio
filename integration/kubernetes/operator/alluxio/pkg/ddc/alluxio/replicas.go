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

	"github.com/Alluxio/alluxio/pkg/utils"
	"k8s.io/client-go/util/retry"
)

// SyncReplicas compare target replicas and current replicas
func (e *AlluxioEngine) SyncReplicas(replicas *int32) (err error) {

	if *e.Config.Replicas != *replicas {
		e.Log.Info("Sync replicas because the current replicas and desired replicas are different.", "current", e.Config.Replicas, "expected", replicas)

		err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			dataset, err := utils.GetDataset(e.Client, e.Config.Name, e.Config.Namespace)
			if err != nil {
				e.Log.Error(err, "Failed to get dataset")
				return err
			}

			desiredNumber, err := e.SetupWorkers(dataset)
			if err != nil {
				e.Log.Error(err, "Failed to sync the cache")
				return err
			}

			e.Log.Info("SyncReplicas", "desiredNumber", desiredNumber)

			// setup runtime desired workers
			runtime, err := utils.GetRuntime(e.Client, e.Config.Name, e.Config.Namespace)
			if err != nil {
				e.Log.Error(err, "Failed to get the runtime")
				return err
			}

			runtimeToUpdate := runtime.DeepCopy()
			runtimeToUpdate.Status.DesiredWorkerNumberScheduled = desiredNumber
			if !reflect.DeepEqual(runtime.Status, runtimeToUpdate.Status) {
				err = e.Client.Status().Update(context.TODO(), runtimeToUpdate)
				if err != nil {
					e.Log.Error(err, "Failed to update the runtime")
					return err
				}
			} else {
				e.Log.Info("Do nothing because the runtime status is not changed.")
			}

			return nil
		})

		if err != nil {
			return err
		}

		e.Config.Replicas = replicas
	} else {
		e.Log.Info("No need to sync replicas, because they are the same.", "current", e.Config.Replicas, "expected", replicas)
	}

	return nil
}
