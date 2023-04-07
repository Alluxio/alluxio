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
	data "github.com/Alluxio/alluxio/api/v1alpha1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NewDatasetCachedCondition creates a new Cache condition.
func NewDatasetCachedCondition(conditionType data.CacheConditionType, reason, message string, status v1.ConditionStatus) data.CacheCondition {
	return data.CacheCondition{
		Type:               conditionType,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		LastUpdateTime:     metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

// SetDatasetCondition updates the dataset to include the provided condition.
// If the condition that we are about to add already exists
// and has the same status and reason then we are not going to update.
func UpdateDatasetCachedCondition(conditions []data.CacheCondition, condition data.CacheCondition) []data.CacheCondition {
	// currentCond := GetDatasetCachedCondition(conditions, condition.Type)

	// conditions = trimDatasetCachedConditions(conditions)

	index, oldCondtion := GetDatasetCachedCondition(conditions, condition.Type)

	if oldCondtion == nil {
		conditions = append(conditions, condition)
		return conditions
	}

	// We are updating an existing condition, so we need to check if it has changed.
	if condition.Status == oldCondtion.Status {
		condition.LastTransitionTime = oldCondtion.LastTransitionTime
	}

	conditions[index] = condition
	return conditions
}

func GetDatasetCachedCondition(conditions []data.CacheCondition,
	condType data.CacheConditionType) (index int, condition *data.CacheCondition) {

	if conditions == nil {
		return -1, nil
	}
	for i := range conditions {
		if conditions[i].Type == condType {
			return i, &conditions[i]
		}
	}
	return -1, nil
}

func trimDatasetCachedConditions(conditions []data.CacheCondition) []data.CacheCondition {
	knownConditions := map[data.CacheConditionType]bool{}
	newConditions := []data.CacheCondition{}
	for _, condition := range conditions {
		if _, found := knownConditions[condition.Type]; !found {
			newConditions = append(newConditions, condition)
			knownConditions[condition.Type] = true
		}
	}

	return newConditions
}

func IsDatasetCachedConditionExist(conditions []data.CacheCondition,
	cond data.CacheCondition) (found bool) {

	condType := cond.Type
	index, existCond := GetDatasetCachedCondition(conditions, condType)
	if index != -1 {
		if existCond.Status == v1.ConditionTrue {
			found = true
		}
	}

	return found
}
