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
	"sort"
	"strings"

	"github.com/Alluxio/alluxio/pkg/common"
	units "github.com/docker/go-units"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Node capacity info
type NodeInfo struct {
	name                     string
	node                     *corev1.Node
	availableStorageCapacity uint64
}

// Sort the NodeInfo by
type SortNodeInfoByCapacity []NodeInfo

func (n NodeInfo) GetDetails() *corev1.Node {
	return n.node
}

func (n NodeInfo) GetName() string {
	return n.name
}

func (n NodeInfo) GetAvailableStorageCapacity() uint64 {
	return n.availableStorageCapacity
}

func (n NodeInfo) GetAvailableStorageCapacityToGiB() string {
	return fmt.Sprintf("%dGB", n.availableStorageCapacity/1024/1024/1024)
}

func (n NodeInfo) GetAvailableStorageCapacityToMiB() string {
	return fmt.Sprintf("%dMB", n.availableStorageCapacity/1024/1024)
}

func (n NodeInfo) GetAvailableStorageCapacityToKiB() string {
	return fmt.Sprintf("%dKB", n.availableStorageCapacity/1024)
}

// Sort from large to small
func (s SortNodeInfoByCapacity) Len() int      { return len(s) }
func (s SortNodeInfoByCapacity) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s SortNodeInfoByCapacity) Less(i, j int) bool {
	// return s[i].CreatedAt.Before(s[j].CreatedAt)
	// return s[i].LastTransitionTime.Time.After(s[j].LastTransitionTime.Time)
	return s[i].availableStorageCapacity > s[j].availableStorageCapacity
}

// make sort nodeInfo by capacity
func makeSortNodeInfoByCapacity(nodes []NodeInfo) []NodeInfo {
	newNodes := make(SortNodeInfoByCapacity, 0, len(nodes))
	for _, c := range nodes {
		newNodes = append(newNodes, c)
	}
	sort.Sort(newNodes)
	return []NodeInfo(newNodes)
}

// Get selected node
func getSelectedNodelistAndCapacity(nodes []NodeInfo, clusterCapacity uint64, datasetCapacity uint64) (selectedNodes []NodeInfo, currentCapacity uint64) {
	currentCapacity = clusterCapacity
	if currentCapacity <= datasetCapacity {
		selectedNodes = nodes
	} else {
		selectedNodes = []NodeInfo{}
		currentCapacity = 0
		// index := 0
		for i := 0; i < len(nodes); i++ {
			if currentCapacity < datasetCapacity {
				selectedNodes = append(selectedNodes, nodes[i])
				currentCapacity += nodes[i].availableStorageCapacity
			} else {
				// TODO (cheyang) Optimize
				break
			}
		}
	}

	// Fot now, show the current capacity
	// if currentCapacity > datasetCapacity {
	// 	currentCapacity = datasetCapacity
	// }

	return selectedNodes, currentCapacity
}

// Get AvailableStorageCapacity
func (b *TemplateEngine) GetAvailableStorageCapacityOfNode(node corev1.Node) (available uint64, err error) {

	// 1. Get available memory resource in node
	totalAvailableStorage := node.Status.Allocatable[corev1.ResourceMemory]
	if common.CacheStoreType == "DISK" {
		totalAvailableStorage = node.Status.Allocatable[corev1.ResourceEphemeralStorage]
	}
	b.Log.Info("Get totalAvailableStorage of the node", "node", node.Name,
		"CacheStoreType", common.CacheStoreType,
		"totalAvailableStorage", totalAvailableStorage.Value())
	// allocatableStorage := uint64(float64(&totalAvailableStorage).Value()) * common.PercentageOfNodeStorageCapacity / 100)
	// m := int64(math.Ceil(float64(memory.Value()) / float64(divisor.Value())))
	//  	return strconv.FormatInt(m, 10), nil
	allocatableStorage := uint64(float64(totalAvailableStorage.Value()) * common.PercentageOfNodeStorageCapacity / 100)
	knownDatasetStorageMap := map[string]resource.Quantity{}

	var usedByDataset uint64 = 0

	reserved, err := units.RAMInBytes(common.ReservedNodeStorageCapacity)
	if err != nil {
		return 0, err
	}

	// 2. Get used by dataset
	for _, runtime := range common.RUNTIMES {
		labelNamePrefix := common.LabelAnnotationStorageCapacityPrefix + "raw-" + runtime + "-"
		for label, value := range node.Labels {
			if label == labelNamePrefix+b.Config.Name {
				b.Log.V(1).Info("The node is skipped due to the dataset is already here",
					"dataset",
					b.Config.Name,
					"node",
					node.Name)
				return 0, nil
			}

			if strings.Contains(label, labelNamePrefix) {
				q, err := resource.ParseQuantity(value)
				if err != nil {
					return 0, err
				}
				knownDatasetStorageMap[label] = q
			}
		}
	}

	for _, value := range knownDatasetStorageMap {
		usedByDataset += uint64(value.Value())
	}

	if allocatableStorage < usedByDataset+uint64(reserved) {
		available = 0
	} else {
		available = allocatableStorage - usedByDataset - uint64(reserved)
	}

	b.Log.Info("Get available storage from Node",
		"node", node.Name,
		"allocatableStorage", units.BytesSize(float64(allocatableStorage)),
		"usedByDataset", units.BytesSize(float64(usedByDataset)),
		"reserved", units.BytesSize(float64(reserved)),
		"available", units.BytesSize(float64(available)))
	return available, nil

}
