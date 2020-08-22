package v1alpha1

import (
	"fmt"
	"strings"

	datav1alpha1 "github.com/Alluxio/pillars/api/v1alpha1"
	"github.com/Alluxio/pillars/pkg/common"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

/**
* construct ddc runtime for the dataset
 */
func constructRuntimeForDataset(dataset datav1alpha1.Dataset) (runtime datav1alpha1.Runtime, err error) {
	// Use the same name for dataset and cache runtime
	// we will support default value in k8s 1.16
	// https://github.com/kubernetes-sigs/controller-tools/pull/323/files
	runtimeType := dataset.Spec.Runtime
	if runtimeType == "" {
		runtimeType = common.DefaultDDCRuntime
	}
	runtime = datav1alpha1.Runtime{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dataset.Name,
			Namespace: dataset.Namespace,
		},
		Spec: datav1alpha1.RuntimeSpec{
			Type:             runtimeType,
			TemplateFileName: fmt.Sprintf("%s-%s-template", dataset.Name, strings.ToLower(runtimeType)),
		},
		Status: datav1alpha1.RuntimeStatus{},
	}

	return runtime, nil
}
