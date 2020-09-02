package common

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"

	data "github.com/Alluxio/alluxio/api/v1alpha1"
)

type ReconcileRequestContext struct {
	context.Context
	Log logr.Logger
	types.NamespacedName
	*data.Runtime
	*data.Dataset
}
