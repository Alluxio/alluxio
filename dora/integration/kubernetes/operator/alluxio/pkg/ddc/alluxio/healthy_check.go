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
	"fmt"
	"reflect"

	data "github.com/Alluxio/alluxio/api/v1alpha1"
	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/utils"

	"k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
)

// HealthyCheck checks the status of the runtime on time
func (e *AlluxioEngine) HealthyCheck() (err error) {

	// 1. Check the healthy of the master
	err = e.checkMasterHealthy()
	if err != nil {
		e.Log.Error(err, "The master is not healthy")
		return
	}

	// 2. Check the healthy of the workers
	err = e.checkWorkersHealthy()
	if err != nil {
		e.Log.Error(err, "The workers are not healthy")
		return
	}

	// 3. Check the healthy of the fuse
	err = e.checkFuseHealthy()
	if err != nil {
		e.Log.Error(err, "The fuse is not healthy")
		return
	}

	return
}

// checkMasterHealthy checks the master healthy
func (e *AlluxioEngine) checkMasterHealthy() (err error) {
	var (
		masterName string = e.Config.Name + "-" + e.Type() + "-master"
		namespace  string = common.ALLUXIO_NAMESPACE
	)

	healthy := false
	master, err := e.getMasterStatefulset(masterName, namespace)
	if err != nil {
		return err
	}

	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// healthy = false
		runtime, err := utils.GetRuntime(e.Client, e.Config.Name, e.Config.Namespace)
		if err != nil {
			return err
		}

		runtimeToUpdate := runtime.DeepCopy()

		if master.Status.Replicas != master.Status.ReadyReplicas {
			if len(runtimeToUpdate.Status.Conditions) == 0 {
				runtimeToUpdate.Status.Conditions = []data.RuntimeCondition{}
			}
			cond := utils.NewRuntimeCondition(data.RuntimeMasterReady, "The master is not ready.",
				fmt.Sprintf("The master %s in %s is not ready.", master.Name, master.Namespace), v1.ConditionFalse)
			_, oldCond := utils.GetRuntimeCondition(runtimeToUpdate.Status.Conditions, cond.Type)

			if oldCond == nil || oldCond.Type != cond.Type {
				runtimeToUpdate.Status.Conditions =
					utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions,
						cond)
			}
			runtimeToUpdate.Status.MasterPhase = data.RuntimePhaseNotReady

			return err
		} else {
			cond := utils.NewRuntimeCondition(data.RuntimeMasterReady, "The master is ready.",
				"The master is ready.", v1.ConditionTrue)
			_, oldCond := utils.GetRuntimeCondition(runtimeToUpdate.Status.Conditions, cond.Type)

			if oldCond == nil || oldCond.Type != cond.Type {
				runtimeToUpdate.Status.Conditions =
					utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions,
						cond)
			}
			runtimeToUpdate.Status.MasterPhase = data.RuntimePhaseReady
			healthy = true
		}

		if !reflect.DeepEqual(runtime.Status, runtimeToUpdate.Status) {
			err = e.Client.Status().Update(context.TODO(), runtimeToUpdate)
			if err != nil {
				e.Log.Error(err, "Failed to update the runtime")
			}
		}

		return nil
	})

	// runtimeToUpdate.Status.MasterNumberReady = uint32(master.Status.ReadyReplicas)

	if err != nil {
		e.Log.Error(err, "Failed update runtime")
		return err
	}

	if !healthy {
		err = fmt.Errorf("The master %s in %s is not ready. The expected number is %d, the actual number is %d",
			master.Name,
			master.Namespace,
			master.Status.Replicas,
			master.Status.ReadyReplicas)
	}

	return err

}

// check workers number changed
func (e *AlluxioEngine) checkWorkersNumberChanged() (changed bool, err error) {
	var (
		namespace  string = common.ALLUXIO_NAMESPACE
		workerName string = e.Config.Name + "-" + e.Type() + "-worker"
	)

	// Check the status of workers
	workers, err := e.getDaemonset(workerName, namespace)
	if err != nil {
		return changed, err
	}

	runtime, err := utils.GetRuntime(e.Client, e.Config.Name, e.Config.Namespace)
	if err != nil {
		return changed, err
	}

	if runtime.Status.DesiredWorkerNumberScheduled != uint32(workers.Status.NumberReady) {
		changed = true
		e.Log.Info(fmt.Sprintf("The daemonset %s in %s are changed from number %d to number %d",
			workers.Name,
			workers.Namespace,
			runtime.Status.DesiredWorkerNumberScheduled,
			workers.Status.NumberReady))
	} else {
		changed = false
	}

	return changed, nil
}

// checkWorkersHealthy checks the worker healthy
func (e *AlluxioEngine) checkWorkersHealthy() (err error) {
	var (
		namespace  string = common.ALLUXIO_NAMESPACE
		workerName string = e.Config.Name + "-" + e.Type() + "-worker"
	)

	// Check the status of workers
	workers, err := e.getDaemonset(workerName, namespace)
	if err != nil {
		return err
	}

	healthy := false
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {

		runtime, err := utils.GetRuntime(e.Client, e.Config.Name, e.Config.Namespace)
		if err != nil {
			return err
		}

		runtimeToUpdate := runtime.DeepCopy()

		if workers.Status.NumberReady != workers.Status.DesiredNumberScheduled {
			if len(runtimeToUpdate.Status.Conditions) == 0 {
				runtimeToUpdate.Status.Conditions = []data.RuntimeCondition{}
			}
			cond := utils.NewRuntimeCondition(data.RuntimeWorkersReady, "The workers are not ready.",
				fmt.Sprintf("The daemonset %s in %s are not ready, the desired number %d, and the real number %d",
					workers.Name,
					workers.Namespace,
					workers.Status.DesiredNumberScheduled,
					workers.Status.NumberReady), v1.ConditionFalse)

			_, oldCond := utils.GetRuntimeCondition(runtimeToUpdate.Status.Conditions, cond.Type)

			if oldCond == nil || oldCond.Type != cond.Type {
				runtimeToUpdate.Status.Conditions =
					utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions,
						cond)
			}

			runtimeToUpdate.Status.WorkerPhase = data.RuntimePhaseNotReady

			// runtimeToUpdate.Status.DesiredWorkerNumberScheduled
			// runtimeToUpdate.Status.WorkerPhase = data.RuntimePhaseNotReady

			return err
		} else {
			healthy = true
			cond := utils.NewRuntimeCondition(data.RuntimeWorkersReady, "The workers are ready.",
				"The workers are ready", v1.ConditionTrue)

			_, oldCond := utils.GetRuntimeCondition(runtimeToUpdate.Status.Conditions, cond.Type)

			if oldCond == nil || oldCond.Type != cond.Type {
				runtimeToUpdate.Status.Conditions =
					utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions,
						cond)
			}
			runtimeToUpdate.Status.WorkerPhase = data.RuntimePhaseReady
		}
		// runtimeToUpdate.Status.DesiredWorkerNumberScheduled = uint32(workers.Status.DesiredNumberScheduled)
		runtimeToUpdate.Status.WorkerNumberReady = uint32(workers.Status.NumberReady)
		runtimeToUpdate.Status.WorkerNumberAvailable = uint32(workers.Status.NumberAvailable)
		if !reflect.DeepEqual(runtime.Status, runtimeToUpdate.Status) {
			err = e.Client.Status().Update(context.TODO(), runtimeToUpdate)
			if err != nil {
				e.Log.Error(err, "Failed to update the runtime")
			}
		}

		return nil
	})

	if err != nil {
		e.Log.Error(err, "Failed update runtime")
		return err
	}

	if !healthy {
		err = fmt.Errorf("The daemonset %s in %s are not ready, the desired number %d, and the real number %d",
			workers.Name,
			workers.Namespace,
			workers.Status.DesiredNumberScheduled,
			workers.Status.NumberReady)
	}

	return err

}

// checkFuseHealthy checks the fuse healthy
func (e *AlluxioEngine) checkFuseHealthy() (err error) {
	var (
		namespace string = common.ALLUXIO_NAMESPACE
		fuseName  string = e.Config.Name + "-" + e.Type() + "-fuse"
	)

	fuses, err := e.getDaemonset(fuseName, namespace)
	if err != nil {
		return err
	}

	healthy := false
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {

		runtime, err := utils.GetRuntime(e.Client, e.Config.Name, e.Config.Namespace)
		if err != nil {
			return err
		}

		runtimeToUpdate := runtime.DeepCopy()

		if fuses.Status.NumberReady != fuses.Status.DesiredNumberScheduled {
			if len(runtimeToUpdate.Status.Conditions) == 0 {
				runtimeToUpdate.Status.Conditions = []data.RuntimeCondition{}
			}
			cond := utils.NewRuntimeCondition(data.RuntimeFusesReady, "The Fuses are not ready.",
				fmt.Sprintf("The daemonset %s in %s are not ready, the desired number %d, and the real number %d",
					fuses.Name,
					fuses.Namespace,
					fuses.Status.DesiredNumberScheduled,
					fuses.Status.NumberReady), v1.ConditionFalse)
			_, oldCond := utils.GetRuntimeCondition(runtimeToUpdate.Status.Conditions, cond.Type)

			if oldCond == nil || oldCond.Type != cond.Type {
				runtimeToUpdate.Status.Conditions =
					utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions,
						cond)
			}

			runtimeToUpdate.Status.FusePhase = data.RuntimePhaseNotReady
			return err
		} else {
			healthy = true
			runtimeToUpdate.Status.FusePhase = data.RuntimePhaseReady
			cond := utils.NewRuntimeCondition(data.RuntimeFusesReady, "The Fuses are ready.",
				"The fuses are ready", v1.ConditionFalse)
			_, oldCond := utils.GetRuntimeCondition(runtimeToUpdate.Status.Conditions, cond.Type)

			if oldCond == nil || oldCond.Type != cond.Type {
				runtimeToUpdate.Status.Conditions =
					utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions,
						cond)
			}
		}

		runtimeToUpdate.Status.FuseNumberReady = uint32(fuses.Status.NumberReady)
		runtimeToUpdate.Status.FuseNumberAvailable = uint32(fuses.Status.NumberAvailable)
		if !reflect.DeepEqual(runtime.Status, runtimeToUpdate.Status) {
			err = e.Client.Status().Update(context.TODO(), runtimeToUpdate)
			if err != nil {
				e.Log.Error(err, "Failed to update the runtime")
			}
		}

		return nil
	})

	if err != nil {
		e.Log.Error(err, "Failed update runtime")
		return err
	}

	if !healthy {
		err = fmt.Errorf("The daemonset %s in %s are not ready, the desired number %d, and the real number %d",
			fuses.Name,
			fuses.Namespace,
			fuses.Status.DesiredNumberScheduled,
			fuses.Status.NumberReady)
	}
	return err

}
