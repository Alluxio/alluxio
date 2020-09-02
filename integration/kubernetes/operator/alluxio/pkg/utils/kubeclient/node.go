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
