package base

import (
	"context"

	"github.com/Alluxio/alluxio/pkg/utils/kubeclient"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Caculate cache capacity
func (b *TemplateEngine) GetNewAvailableForCacheInCluster() (totalClusterCapacity uint64, err error) {
	var (
		nodeList        *corev1.NodeList = &corev1.NodeList{}
		avaliblePerNode uint64           = 0
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

		avaliblePerNode, err = b.GetAvailableStorageCapacityOfNode(node)
		if err != nil {
			return
		}

		// The node with taints will be skipped
		if len(node.Spec.Taints) > 0 {
			b.Log.V(1).Info("Skip the node because it's tainted", "node", node.Name)
			continue
		}

		if avaliblePerNode == 0 {
			b.Log.V(1).Info("Skip the node because its capacity is 0", "node", node.Name)
			continue
		}

		totalClusterCapacity += avaliblePerNode
	}

	return totalClusterCapacity, err
}
