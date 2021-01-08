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
