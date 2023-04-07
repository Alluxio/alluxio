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
	"fmt"
	"reflect"

	data "github.com/Alluxio/alluxio/api/v1alpha1"
	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/ddc/configs"
	"github.com/Alluxio/alluxio/pkg/utils"
	"github.com/Alluxio/alluxio/pkg/utils/kubeclient"
	units "github.com/docker/go-units"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/retry"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"context"
)

type TemplateEngine struct {
	Implement
	Id string
	client.Client
	Log    logr.Logger
	Config *configs.Config
}

func NewTemplateEngine(impl Implement,
	id string,
	client client.Client,
	config *configs.Config,
	log logr.Logger) *TemplateEngine {
	b := &TemplateEngine{
		Implement: impl,
		Id:        id,
		Config:    config,
		Client:    client,
		// Log:       log,
	}
	b.Log = log.WithValues("engine", b.Implement.Type()).WithValues("id", id)
	return b
}

// setup worker
func (b *TemplateEngine) SetupWorkers(dataset *data.Dataset) (expectedWorkerNum uint32, err error) {
	// prepare the ufs
	err = b.PrepareUFS()
	if err != nil {
		return
	}

	toUpdate := dataset.DeepCopy()

	datasetUFSTotalBytes, err := b.TotalStorageBytes()
	if err != nil {
		return
	}
	b.Log.Info("UFS storage", "storage", units.BytesSize(float64(datasetUFSTotalBytes)))

	// // Get the size of dataset which is already loaded into cache
	// var currentCachedCapacityBytes uint64 = 0

	// cachedQuantity := "0"
	// if dataset.Status.CacheStatus.CacheStates[data.CacheCapacity] != "" {
	// 	cachedQuantity = dataset.Status.CacheStatus.CacheStates[data.CacheCapacity]
	// }
	// value, err := units.RAMInBytes(cachedQuantity)
	// if err != nil {
	// 	return expectedWorkerNum, err
	// }
	// currentCachedCapacityBytes = uint64(value)

	if datasetUFSTotalBytes > 0 {
		ufsTotal := "0"
		if dataset.Status.UfsTotal != "" {
			ufsTotal = dataset.Status.UfsTotal
		}
		ramInBytes, err := units.RAMInBytes(ufsTotal)
		if err != nil {
			return expectedWorkerNum, err
		}
		if ramInBytes != int64(datasetUFSTotalBytes) {
			toUpdate.Status.UfsTotal = units.BytesSize(float64(datasetUFSTotalBytes))
			toUpdate.Status.Total = units.BytesSize(float64(datasetUFSTotalBytes * (uint64)(*dataset.Spec.Replicas)))
			err = b.Client.Status().Update(context.TODO(), toUpdate)
			if err != nil {
				b.Log.Error(err, "Failed to update")
				return expectedWorkerNum, err
			}
		}

		// expectedWorkerNum, err = b.AssignNodesToCache(datasetUFSTotalBytes, currentCachedCapacityBytes)
		total := (uint64)((float64)(datasetUFSTotalBytes*(uint64)(*dataset.Spec.Replicas)) / b.Implement.LowWaterMarkRatio())

		b.Log.Info("assign nodes to cache", "datasetUFSTotalBytes", datasetUFSTotalBytes,
			"replicas", *dataset.Spec.Replicas,
			"lowWaterMarkRatio", b.Implement.LowWaterMarkRatio(),
			"total", total)

		expectedWorkerNum, err = b.AssignNodesToCache(total)
		if err != nil {
			b.Log.Error(err, "Failed to assign nodes to cache")
			return expectedWorkerNum, err
		}
	}

	return
}

// Find the nodes to put the worker for caching
func (b *TemplateEngine) AssignNodesToCache(datasetUFSTotalBytes uint64) (expectedWorkerNum uint32, err error) {
	var (
		nodeList                      *corev1.NodeList = &corev1.NodeList{}
		totalAvailableClusterCapacity uint64           = 0
		availablePerNode              uint64           = 0
		currentCachedCapacityBytes    uint64           = 0
	)
	// 0. The current cache capacity and worker number
	currentCachedCapacityBytes, existingWorkerNum, err := b.GetCurrentCachedCapacity()
	if err != nil {
		return
	}

	err = b.List(context.TODO(), nodeList, &client.ListOptions{})
	if err != nil {
		return
	}

	if currentCachedCapacityBytes >= datasetUFSTotalBytes {
		b.Log.Info("No need assign new nodes because current cacheCapacity can handle the UFS storage", "currentCachedCapacityBytes", currentCachedCapacityBytes,
			"datasetUFSTotalBytes", datasetUFSTotalBytes)
		return existingWorkerNum, nil
	}

	// 1.select the nodes
	// TODO(cheyang) Need consider node selector
	nodeInfos := []NodeInfo{}
	for _, node := range nodeList.Items {
		if b.Config.DatasetConfig.NodeAffinity != nil {
			if b.Config.DatasetConfig.NodeAffinity.Required != nil {
				terms := b.Config.NodeAffinity.Required.NodeSelectorTerms
				if !v1helper.MatchNodeSelectorTerms(terms, labels.Set(node.Labels), nil) {
					b.Log.V(1).Info("Node is skipped because it can't meet node selector terms", "node", node.Name)
					continue
				}
			}
		}

		if !kubeclient.IsReady(node) {
			b.Log.V(1).Info("Node is skipped because it is not ready", "node", node.Name)
			continue
		}

		// nodes = append(nodes, &node)
		availablePerNode, err = b.GetAvailableStorageCapacityOfNode(node)
		if err != nil {
			return
		}

		if availablePerNode == 0 {
			b.Log.Info("Skip the node because its capacity is 0", "node", node.Name)
			continue
		}

		var info NodeInfo = NodeInfo{
			name: node.Name,
			node: &node,
			availableStorageCapacity: availablePerNode,
		}

		// The node with taints will be skipped
		if len(node.Spec.Taints) > 0 {
			b.Log.Info("Skip the node because it's tainted", "node", node.Name)
			continue
		}

		nodeInfos = append(nodeInfos, info)
		totalAvailableClusterCapacity += availablePerNode
		b.Log.V(1).Info("Node to add",
			"node", node.Name,
			"available storage", availablePerNode,
			"totalAvailableClusterCapacity", totalAvailableClusterCapacity)
	}

	nodeInfos = makeSortNodeInfoByCapacity(nodeInfos)
	var needLoaded uint64 = 0
	needLoaded = datasetUFSTotalBytes - currentCachedCapacityBytes

	b.Log.Info("before getSelectedNodelistAndCapacity", "totalAvailableClusterCapacity", totalAvailableClusterCapacity,
		"needLoaded", needLoaded)
	selectedNodes, capacity := getSelectedNodelistAndCapacity(nodeInfos, totalAvailableClusterCapacity, needLoaded)
	b.Log.Info("after getSelectedNodelistAndCapacity", "selectedNodes", selectedNodes,
		"capacity", capacity)
	expectedWorkerNum = uint32(len(selectedNodes)) + existingWorkerNum

	// 2.Add label to the selected node
	err = b.LabelCachedNodes(selectedNodes, capacity)
	if err != nil {
		return expectedWorkerNum, err
	}

	var nonCacheable uint64 = 0
	if datasetUFSTotalBytes > currentCachedCapacityBytes+capacity {
		nonCacheable = datasetUFSTotalBytes - currentCachedCapacityBytes - capacity
	}

	// 3.Update the runtime status
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		runtime, err := utils.GetRuntime(b.Client, b.Config.Name, b.Config.Namespace)
		if err != nil {
			return err
		}
		runtimeToUpdate := runtime.DeepCopy()

		if len(runtimeToUpdate.Status.CacheStates) == 0 {
			runtimeToUpdate.Status.CacheStates = map[data.CacheStateName]string{}
		}

		runtimeToUpdate.Status.CacheStates[data.CacheCapacity] = units.BytesSize(float64(capacity))
		// runtimeToUpdate.Status.CacheStates[data.Cacheable] = units.BytesSize(float64(0))
		runtimeToUpdate.Status.CacheStates[data.Cached] = units.BytesSize(float64(0))
		runtimeToUpdate.Status.CacheStates[data.NonCacheable] = units.BytesSize(float64(nonCacheable))
		runtimeToUpdate.Status.CacheStates[data.CachedPercentage] = fmt.Sprintf("%.2f %%", 0.00)
		// runtimeToUpdate.Status.CacheStates[data.Cacheable]
		err = b.Client.Status().Update(context.TODO(), runtimeToUpdate)
		if err != nil {
			b.Log.Error(err, "Failed update runtime")
		}
		return err
	})
	// TODO:(cheyang)Algorithm needs optimization
	return expectedWorkerNum, err

}

// Remove the nodes to put the worker for caching
func (b *TemplateEngine) RemoveCacheNodes() (err error) {
	var (
		nodeList          *corev1.NodeList = &corev1.NodeList{}
		labelNameToAddRaw                  = common.LabelAnnotationStorageCapacityPrefix + "raw-" + b.Type() + "-" + b.Config.Name
		labelNameToAdd                     = common.LabelAnnotationStorageCapacityPrefix + "human-" + b.Type() + "-" + b.Config.Name
		labelName                          = common.LabelAnnotationStorageCapacityPrefix + b.Type() + "-" + b.Config.Name
		labelCommonName                    = common.LabelAnnotationStorageCapacityPrefix + b.Config.Name
	)

	err = b.List(context.TODO(), nodeList, &client.ListOptions{})
	if err != nil {
		return
	}

	// 1.select the nodes
	// TODO(cheyang) Need consider node selector
	for _, node := range nodeList.Items {
		// nodes = append(nodes, &node)
		toUpdate := node.DeepCopy()
		if len(toUpdate.Labels) == 0 {
			continue
		}

		delete(toUpdate.Labels, labelNameToAddRaw)
		delete(toUpdate.Labels, labelNameToAdd)
		delete(toUpdate.Labels, labelName)
		delete(toUpdate.Labels, labelCommonName)

		if len(toUpdate.Labels) < len(node.Labels) {
			err := b.Client.Update(context.TODO(), toUpdate)
			if err != nil {
				return err
			}
		}
	}

	return

}

// label the node to cache
func (b *TemplateEngine) LabelCachedNodes(selectedNodes []NodeInfo, cacheCapacity uint64) (err error) {
	// labelNameToAdd := common.LabelAnnotationStorageCapacityPrefix + b.Config.Name
	var (
		needed uint64 = cacheCapacity
	)
	for _, n := range selectedNodes {

		allocated, err := b.labelCachedNode(n, needed)

		if err != nil {
			b.Log.Error(err, "LabelCachedNodes")
			return err
		}

		needed = needed - allocated

		// if toUpdate.Labels == nil {
		// 					toUpdate.Labels = make(map[string]string)
		// 				}
		// 			toUpdate.Labels[apps.DefaultDaemonSetUniqueLabelKey] = keepCur.Labels[apps.DefaultDaemonSetUniqueLabelKey]
		// 		_, err = dsc.kubeClient.CoreV1().Pods(ds.Namespace).Update(toUpdate)

		// labels := node.Labels
		// for _, label := range labels {

		// }
	}

	return err
}

func (b *TemplateEngine) labelCachedNode(selectedNode NodeInfo, needed uint64) (allocated uint64, err error) {
	var (
		labelNameToAddRaw = common.LabelAnnotationStorageCapacityPrefix + "raw-" + b.Type() + "-" + b.Config.Name
		// labelNameToAddKiB = common.LabelAnnotationStorageCapacityPrefix + "KB-" + b.Type() + "-" + b.Config.Name
		// labelNameToAddMiB = common.LabelAnnotationStorageCapacityPrefix + "MB-" + b.Type() + "-" + b.Config.Name
		// labelNameToAddGiB = common.LabelAnnotationStorageCapacityPrefix + "GB-" + b.Type() + "-" + b.Config.Name
		labelNameToAdd  = common.LabelAnnotationStorageCapacityPrefix + "human-" + b.Type() + "-" + b.Config.Name
		labelName       = common.LabelAnnotationStorageCapacityPrefix + b.Type() + "-" + b.Config.Name
		labelCommonName = common.LabelAnnotationStorageCapacityPrefix + b.Config.Name
	)

	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		nodeName := selectedNode.GetName()
		node, err := kubeclient.GetNode(b.Client, nodeName)
		if node == nil {
			return err
		}
		toUpdate := node.DeepCopy()
		if toUpdate.Labels == nil {
			toUpdate.Labels = make(map[string]string)
		}

		if needed < selectedNode.GetAvailableStorageCapacity() {
			value := units.BytesSize(float64(needed))
			toUpdate.Labels[labelNameToAdd] = value
			// For now, we are not able to set node label to pods due to https://github.com/kubernetes/kubernetes/issues/40610
			// We have set it in pod level
			err := b.SetRuntimeMaxMemory(selectedNode.GetName(), value)
			if err != nil {
				return err
			}
			toUpdate.Labels[labelNameToAddRaw] = fmt.Sprintf("%d", needed)
			allocated = needed
			toUpdate.Labels[labelName] = "true"
			toUpdate.Labels[labelCommonName] = "true"
		} else {
			value := units.BytesSize(float64(selectedNode.GetAvailableStorageCapacity()))
			toUpdate.Labels[labelNameToAdd] = value
			err := b.SetRuntimeMaxMemory(selectedNode.GetName(), value)
			if err != nil {
				return err
			}
			toUpdate.Labels[labelNameToAddRaw] = fmt.Sprintf("%d", selectedNode.GetAvailableStorageCapacity())
			allocated = selectedNode.GetAvailableStorageCapacity()
			toUpdate.Labels[labelName] = "true"
			toUpdate.Labels[labelCommonName] = "true"
		}

		// toUpdate.Labels[labelNameToAdd] = "true"
		err = b.Client.Update(context.TODO(), toUpdate)
		if err != nil {
			b.Log.Error(err, "LabelCachedNodes")
			return err
		}
		return nil
	})

	if err != nil {
		b.Log.Error(err, "LabelCacheNode")
		return allocated, err
	}

	return allocated, nil

}

// Setup the ddc engine
func (b *TemplateEngine) Setup(ctx common.ReconcileRequestContext) (ready bool, err error) {

	var shouldSetupMaster, masterReady bool
	b.Log.V(1).Info("Get", "Runtime", ctx.Runtime)
	b.Log.V(1).Info("Get", "Dataset", ctx.Dataset)
	runtime := ctx.Runtime
	dataset := ctx.Dataset

	// If the dataset condition is created, it means the dataset is already setup
	index, _ := utils.GetDatasetCachedCondition(dataset.Status.CacheStatus.Conditions, data.Ready)
	if index != -1 {
		b.Log.V(1).Info("The runtime is already setup.")
		ready = true
		return ready, nil
	}

	switch runtime.Status.MasterPhase {
	case data.RuntimePhaseNone:
		shouldSetupMaster = true
	default:
		shouldSetupMaster = false
	}

	// 1. Setup Master
	if shouldSetupMaster {
		desiredNum, err := b.SetupMaster()
		if err != nil {
			b.Log.Error(err, "SetupMaster")
			return ready, err
		}

		// 1.1 Update the runtime
		err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			runtimeToUpdate, err := utils.GetRuntime(b.Client, b.Config.Name, b.Config.Namespace)
			if err != nil {
				return err
			}
			runtimeToUpdate.Status.MasterPhase = data.RuntimePhaseNotReady
			runtimeToUpdate.Status.DesiredMasterNumberScheduled = desiredNum
			runtimeToUpdate.Status.ValueFileConfigmap = dataset.Name + "-" + b.Type() + "-values"
			if len(runtimeToUpdate.Status.Conditions) == 0 {
				runtimeToUpdate.Status.Conditions = []data.RuntimeCondition{}
			}
			cond := utils.NewRuntimeCondition(data.RuntimeMasterInitialized, data.RuntimeMasterInitializedReason,
				"The master is initialized.", corev1.ConditionTrue)
			runtimeToUpdate.Status.Conditions =
				utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions,
					cond)

			return b.Client.Status().Update(ctx.Context, runtimeToUpdate)
		})

		if err != nil {
			b.Log.Error(err, "Update runtime status")
			return ready, err
		}

		// 1.2 update the status of dataset
		err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			datasetToUpdate, err := utils.GetDataset(b.Client, b.Config.Name, b.Config.Namespace)
			if err != nil {
				return err
			}
			datasetToUpdate.Status.Phase = data.PendingDatasetPhase
			if len(datasetToUpdate.Status.CacheStatus.Conditions) == 0 {
				datasetToUpdate.Status.CacheStatus.Conditions = []data.CacheCondition{}
			}

			cond := utils.NewDatasetCachedCondition(data.Planned,
				data.DatasetPlannedReason,
				"The ddc runtime is creating",
				corev1.ConditionTrue)

			datasetToUpdate.Status.CacheStatus.Conditions =
				utils.UpdateDatasetCachedCondition(datasetToUpdate.Status.CacheStatus.Conditions,
					cond)

			// datasetToUpdate.Status.CacheStatus.Conditions = append(datasetToUpdate.Status.CacheStatus.Conditions,
			// 		data.CacheCondition{
			// 			Type: data.Planned,

			// 		})

			return b.Client.Status().Update(ctx.Context, datasetToUpdate)
		})

		if err != nil {
			b.Log.Error(err, "Update dataset status")
			return ready, err
		}

		return ready, nil
	}

	masterReady, err = b.IsMasterReady(runtime)
	if err != nil {
		return ready, err
	}

	if !masterReady {
		return masterReady, err
	}

	// Update the condition of the runtime
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		runtime, err := utils.GetRuntime(b.Client, b.Config.Name, b.Config.Namespace)
		if err != nil {
			return err
		}
		runtimeToUpdate := runtime.DeepCopy()

		if len(runtimeToUpdate.Status.Conditions) == 0 {
			runtimeToUpdate.Status.Conditions = []data.RuntimeCondition{}
		}
		cond := utils.NewRuntimeCondition(data.RuntimeMasterReady, data.RuntimeMasterReadyReason,
			"The master is ready.", corev1.ConditionTrue)
		runtimeToUpdate.Status.Conditions =
			utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions,
				cond)
		runtimeToUpdate.Status.MasterPhase = data.RuntimePhaseReady
		if !reflect.DeepEqual(runtime.Status, runtimeToUpdate.Status) {
			return b.Client.Status().Update(ctx.Context, runtimeToUpdate)
		}

		return nil
	})

	if err != nil {
		b.Log.Error(err, "Update runtime to master ready")
		return ready, err
	}

	// 2.Check the ufs capacity
	//TODO:(cheyang) sync ufs storage once by now, will be predically in future
	var shouldSetupWorkers bool
	switch runtime.Status.WorkerPhase {
	case data.RuntimePhaseNone:
		shouldSetupWorkers = true
	default:
		shouldSetupWorkers = false
	}

	if shouldSetupWorkers {
		desiredNum, err := b.SetupWorkers(dataset)
		if err != nil {
			b.Log.Error(err, "SetupWorker")
			return ready, err
		}
		err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			runtime, err := utils.GetRuntime(b.Client, b.Config.Name, b.Config.Namespace)
			if err != nil {
				return err
			}
			runtimeToUpdate := runtime.DeepCopy()

			runtimeToUpdate.Status.WorkerPhase = data.RuntimePhaseNotReady
			runtimeToUpdate.Status.DesiredWorkerNumberScheduled = desiredNum
			runtimeToUpdate.Status.FusePhase = data.RuntimePhaseNotReady
			runtimeToUpdate.Status.DesiredFuseNumberScheduled = desiredNum
			if len(runtimeToUpdate.Status.Conditions) == 0 {
				runtimeToUpdate.Status.Conditions = []data.RuntimeCondition{}
			}
			cond := utils.NewRuntimeCondition(data.RuntimeWorkersInitialized, data.RuntimeWorkersInitializedReason,
				"The workers are initialized.", corev1.ConditionTrue)
			runtimeToUpdate.Status.Conditions =
				utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions,
					cond)
			fuseCond := utils.NewRuntimeCondition(data.RuntimeFusesInitialized, data.RuntimeFusesInitializedReason,
				"The fuses are initialized.", corev1.ConditionTrue)
			runtimeToUpdate.Status.Conditions =
				utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions,
					fuseCond)

			if !reflect.DeepEqual(runtime.Status, runtimeToUpdate.Status) {
				return b.Client.Status().Update(ctx.Context, runtimeToUpdate)
			}

			return nil
		})

		if err != nil {
			b.Log.Error(err, "Update runtime")
			return ready, err
		}
	}

	runtime, err = utils.GetRuntime(b.Client, b.Config.Name, b.Config.Namespace)
	if err != nil {
		return ready, err
	}
	workerReady, err := b.AreWorkersReady(runtime)
	if err != nil {
		b.Log.Error(err, "Check if the workers are ready")
		return workerReady, err
	}

	if !workerReady {
		return workerReady, err
	}

	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		runtime, err := utils.GetRuntime(b.Client, b.Config.Name, b.Config.Namespace)
		if err != nil {
			return err
		}
		runtimeToUpdate := runtime.DeepCopy()
		if len(runtimeToUpdate.Status.Conditions) == 0 {
			runtimeToUpdate.Status.Conditions = []data.RuntimeCondition{}
		}
		cond := utils.NewRuntimeCondition(data.RuntimeWorkersReady, data.RuntimeWorkersReadyReason,
			"The workers are ready.", corev1.ConditionTrue)
		runtimeToUpdate.Status.Conditions =
			utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions,
				cond)
		fuseCond := utils.NewRuntimeCondition(data.RuntimeFusesReady, data.RuntimeFusesReadyReason,
			"The fuses are ready.", corev1.ConditionTrue)
		runtimeToUpdate.Status.Conditions =
			utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions,
				fuseCond)

		if !reflect.DeepEqual(runtime.Status, runtimeToUpdate.Status) {
			return b.Client.Status().Update(ctx.Context, runtimeToUpdate)
		}

		return nil
	})
	// 4. Setup the init Runtime status
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		runtime, err := utils.GetRuntime(b.Client, b.Config.Name, b.Config.Namespace)
		if err != nil {
			b.Log.Error(err, "Get Runtime")
			return err
		}
		ready, err = b.UpdateRuntimeStatus(runtime)
		if err != nil {
			b.Log.Error(err, "Update runtime")
			return err
		}

		return nil
	})

	if err != nil {
		b.Log.Error(err, "Update runtime")
		return ready, err
	}

	// 5.Setup the init status of Dataset
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		dataset, err = utils.GetDataset(b.Client, b.Config.Name, b.Config.Namespace)
		if err != nil {
			b.Log.Error(err, "Get dataset")
			return err
		}

		runtime, err := utils.GetRuntime(b.Client, b.Config.Name, b.Config.Namespace)
		if err != nil {
			return err
		}
		datasetToUpdate := dataset.DeepCopy()

		datasetToUpdate.Status.Phase = data.ReadyDatasetPhase
		cond := utils.NewDatasetCachedCondition(data.Ready, data.DatasetReadyReason,
			"The ddc runtime is ready.",
			corev1.ConditionTrue)
		// if dataset.Spec.PrefetchStrategy == data.AlwaysPrefetch {
		// 	cond.Reason = data.DatasetPreLoadingReason
		// 	cond.Type = data.Preloading
		// 	cond.Message = "The ddc runtime is ready, but the data loading is not started"
		// }
		datasetToUpdate.Status.CacheStatus.Conditions = utils.UpdateDatasetCachedCondition(datasetToUpdate.Status.CacheStatus.Conditions,
			cond)

		datasetToUpdate.Status.CacheStatus.CacheStates = runtime.Status.CacheStates
		// datasetToUpdate.Status.CacheStatus.CacheStates =

		if !reflect.DeepEqual(dataset.Status, datasetToUpdate.Status) {
			err = b.Client.Status().Update(ctx, datasetToUpdate)
			if err != nil {
				b.Log.Error(err, "Update dataset")
				return err
			}
		}

		return nil

		// return b.UpdateCacheStateOfDataset(datasetToUpdate)

	})
	if err != nil {
		b.Log.Error(err, "Update dataset")
		return ready, err
	}

	// // 5.Preload the dataset if required
	// if ready && dataset.Spec.PrefetchStrategy == data.AlwaysPrefetch {
	// 	err = b.Implement.Preload(dataset)
	// }

	return

}

// Destroy the engine
func (b *TemplateEngine) Destroy() (err error) {

	err = b.CleanupCache()
	if err != nil {
		return
	}

	err = b.DestroyWorkers()
	if err != nil {
		return
	}

	err = b.DestroyMaster()
	if err != nil {
		return
	}

	err = b.CleanAll()
	return err
}

func (b *TemplateEngine) DestroyWorkers() (err error) {
	return b.RemoveCacheNodes()
}
