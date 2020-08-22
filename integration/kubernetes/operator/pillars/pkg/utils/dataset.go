package utils

import (
	"context"

	data "github.com/Alluxio/alluxio/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

/*
* Get the dataset
 */
func GetDataset(client client.Client, name, namespace string) (*data.Dataset, error) {

	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	var dataset data.Dataset
	if err := client.Get(context.TODO(), key, &dataset); err != nil {
		return nil, err
	}
	return &dataset, nil
}

/*
* Get the runtime
 */
func GetRuntime(client client.Client, name, namespace string) (*data.Runtime, error) {

	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	var runtime data.Runtime
	if err := client.Get(context.TODO(), key, &runtime); err != nil {
		return nil, err
	}
	return &runtime, nil
}
