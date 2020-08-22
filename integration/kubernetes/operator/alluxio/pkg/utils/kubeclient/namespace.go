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
