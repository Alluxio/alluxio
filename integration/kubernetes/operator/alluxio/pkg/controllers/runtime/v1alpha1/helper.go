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

package v1alpha1

import (
	"strconv"

	data "github.com/Alluxio/alluxio/api/v1alpha1"
	common "github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/ddc/configs"
)

/*
* Get or build the desired runtime status from the dataset status
 */
func (r *RuntimeReconciler) getOrBuildDesiredRuntimeStatus(ctx common.ReconcileRequestContext) (*data.RuntimeStatus, error) {

	// if no condition, it indicates the status needs to build
	if len(ctx.Runtime.Status.Conditions) == 0 {

	}

	return &data.RuntimeStatus{}, nil
}

/*
* Get the desired runtime status from the dataset status
 */
func (r *RuntimeReconciler) getActualRuntimeStatus(ctx common.ReconcileRequestContext) (*data.RuntimeStatus, error) {
	return &data.RuntimeStatus{}, nil
}

/*
* Get the dataset
 */
func (r *RuntimeReconciler) getDataset(ctx common.ReconcileRequestContext) (*data.Dataset, error) {
	var dataset data.Dataset
	if err := r.Get(ctx, ctx.NamespacedName, &dataset); err != nil {
		return nil, err
	}
	return &dataset, nil
}

/*
* Get the runtime
 */
func (r *RuntimeReconciler) getRuntime(ctx common.ReconcileRequestContext) (*data.Runtime, error) {
	var runtime data.Runtime
	if err := r.Get(ctx, ctx.NamespacedName, &runtime); err != nil {
		return nil, err
	}
	return &runtime, nil
}

// Determine the action necessary to bring actual state to desired state.
func (r *RuntimeReconciler) determineActionForRuntimeStatus(desired *data.RuntimeStatus,
	actual *data.RuntimeStatus) common.ReconcileAction {
	return common.NeedsNoop
}

// Build the config from context
func (r *RuntimeReconciler) buildConfig(ctx common.ReconcileRequestContext) (config *configs.Config) {
	var (
		dataset *data.Dataset
	)

	if ctx.Dataset != nil {
		dataset = ctx.Dataset
	}

	runtimeName := ctx.Runtime.Spec.Type
	if runtimeName == "" {
		runtimeName = common.DefaultDDCRuntime
	}

	config = &configs.Config{
		Name:         ctx.Runtime.Name,
		Namespace:    ctx.Runtime.Namespace,
		Runtime:      runtimeName,
		TemplateFile: ctx.Runtime.Spec.TemplateFileName,
		Options:      ctx.Runtime.Spec.Options,
	}

	if dataset != nil {
		config.DatasetConfig = configs.DatasetConfig{
			Mounts:           dataset.Spec.Mounts,
			Replicas:         dataset.Spec.Replicas,
			PrefetchStrategy: dataset.Spec.PrefetchStrategy,
			NodeAffinity:     dataset.Spec.NodeAffinity,
		}

		lowWaterMarkRatioStr := dataset.Spec.LowWaterMarkRatio
		lowWaterMarkRatio, err := strconv.ParseFloat(lowWaterMarkRatioStr, 64)
		if err != nil {
			lowWaterMarkRatio = 0.7
		}
		config.DatasetConfig.LowWaterMarkRatio = lowWaterMarkRatio

		highWaterMarkRatioStr := dataset.Spec.HighWaterMarkRatio
		highWaterMarkRatio, err := strconv.ParseFloat(highWaterMarkRatioStr, 64)
		if err != nil {
			highWaterMarkRatio = 0.95
		}
		config.DatasetConfig.HighWaterMarkRatio = highWaterMarkRatio

	}

	return config
}
