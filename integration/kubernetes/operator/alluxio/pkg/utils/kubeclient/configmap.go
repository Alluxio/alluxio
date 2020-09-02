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
