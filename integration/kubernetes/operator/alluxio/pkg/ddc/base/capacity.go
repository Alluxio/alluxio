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
	"context"

	"github.com/Alluxio/alluxio/pkg/utils/kubeclient"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Calculate cache capacity
func (b *TemplateEngine) GetNewAvailableForCacheInCluster() (totalClusterCapacity uint64, err error) {
	var (
		nodeList        *corev1.NodeList = &corev1.NodeList{}
		availablePerNode uint64          = 0
	)

	err = b.List(context.TODO(), nodeList, &client.ListOptions{})
	if err != nil {
		return
	}

	for _, node := range nodeList.Items {
		if !kubeclient.IsReady(node) {
			b.Log.V(1).Info("Node is skipped because it is not ready", "node", node.Name)
			continue
		}

		availablePerNode, err = b.GetAvailableStorageCapacityOfNode(node)
		if err != nil {
			return
		}

		// The node with taints will be skipped
		if len(node.Spec.Taints) > 0 {
			b.Log.V(1).Info("Skip the node because it's tainted", "node", node.Name)
			continue
		}

		if availablePerNode == 0 {
			b.Log.V(1).Info("Skip the node because its capacity is 0", "node", node.Name)
			continue
		}

		totalClusterCapacity += availablePerNode
	}

	return totalClusterCapacity, err
}
