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

	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func IsConfigMapExist(client client.Client, name, namespace string) (found bool, err error) {
	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	cm := &v1.ConfigMap{}

	if err = client.Get(context.TODO(), key, cm); err != nil {
		if apierrs.IsNotFound(err) {
			found = false
			err = nil
		}
	} else {
		found = true
	}
	return found, err
}

func DeleteConfigMap(client client.Client, name, namespace string) (err error) {
	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	found := false

	cm := &v1.ConfigMap{}
	if err = client.Get(context.TODO(), key, cm); err != nil {
		if apierrs.IsNotFound(err) {
			found = false
		} else {
			return
		}
	} else {
		found = true
	}
	if found {
		err = client.Delete(context.TODO(), cm)
	}

	return err
}
