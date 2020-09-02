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

package kubeclient

import (
	"context"

	"k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Get the latest node info
func GetNode(client client.Client, name string) (node *v1.Node, err error) {
	key := types.NamespacedName{
		Name: name,
	}

	node = &v1.Node{}

	if err = client.Get(context.TODO(), key, node); err != nil {
		return nil, err
	}
	return node, err
}

// Check if the node is ready
func IsReady(node v1.Node) (ready bool) {
	ready = true
	for _, condition := range node.Status.Conditions {
		if condition.Type == v1.NodeReady && condition.Status != v1.ConditionTrue {
			ready = false
			break
		}
	}
	return ready
}
