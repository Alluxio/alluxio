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
	"github.com/go-logr/logr"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

type NodeStatusChangedPredicate struct {
	predicate.Funcs
	Log logr.Logger
}

func (ns *NodeStatusChangedPredicate) Update(e event.UpdateEvent) bool {
	return true
}

// nodeShouldRunDDCDaemonPod checks a set of preconditions against a (node, ddcdaemonset) and returns a
// summary. Returned booleans are:
// * wantToRun:
//     Returns true when controller would expect a pod to run on this node and ignores conditions
//     such as DiskPressure or insufficient resource that would cause a daemonset pod not to schedule.
//     This is primarily used to populate daemonset status.
// * shouldSchedule:
//     Returns true when a daemonset should be scheduled to a node if a daemonset pod is not already
//     running on that node.
// * shouldContinueRunning:
//     Returns true when a daemonset should continue running on a node if a daemonset pod is already
//     running on that node.
func (r *DatasetReconciler) nodeShouldRunDDCDaemonPod(node *v1.Node, ds *apps.DaemonSet) (wantToRun, shouldSchedule, shouldContinueRunning bool, err error) {
	return true, true, true, nil
}

func (r *DatasetReconciler) addNode(node *v1.Node) {

}
