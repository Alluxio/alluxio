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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Make sure the namespace exist
func EnsureNamespace(client client.Client, namespace string) (err error) {
	key := types.NamespacedName{
		Name: namespace,
	}

	var ns v1.Namespace

	if err = client.Get(context.TODO(), key, &ns); err != nil {
		if apierrs.IsNotFound(err) {
			return createNamespace(client, namespace)
		} else {
			return err
		}
	}
	return err
}

func createNamespace(client client.Client, namespace string) error {
	created := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}

	return client.Create(context.TODO(), created)
}
