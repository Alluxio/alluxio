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

package ddc

import (
	"fmt"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

// SyncAction indicates different kind of actions in Sync() and Teardown(). Now there are only actions
// about start/kill cache master and worker
type SyncAction string

const (
	ScheduleNodes       SyncAction = "ScheduleNodes"
	RunCacheSystem      SyncAction = "RunCacheSystem"
	TeardownCacheSystem SyncAction = "TeardownCacheSystem"
	FreeNodes           SyncAction = "FreeNodes"
	PreloadCache        SyncAction = "PreloadCache"
	FreeCache           SyncAction = "FreeCache"
)

// SyncResult is the result of sync action.
type SyncResult struct {
	// The associated action of the result
	Action SyncAction
	// The target of the action, now the target can only be:
	//  * CacheSystem
	//  * Cache
	Target interface{}
	// Brief error reason
	Error error
	// Human readable error reason
	Message string
}

// NewSyncResult generates new SyncResult with specific Action and Target
func NewSyncResult(action SyncAction, target interface{}) *SyncResult {
	return &SyncResult{Action: action, Target: target}
}

// Fail fails the SyncResult with specific error and message
func (r *SyncResult) Fail(err error, msg string) {
	r.Error, r.Message = err, msg
}

// EngineSyncResult is the summary result of Sync() and Teardown()
type EngineSyncResult struct {
	// Result of different sync actions
	SyncResults []*SyncResult
	// Error encountered in Sync() and Teardown() that is not already included in SyncResults
	SyncError error
}

// AddSyncResult adds multiple SyncResult to current EngineSyncResult
func (p *EngineSyncResult) AddSyncResult(result ...*SyncResult) {
	p.SyncResults = append(p.SyncResults, result...)
}

// AddEngineSyncResult merges a EngineSyncResult to current one
func (p *EngineSyncResult) AddEngineSyncResult(result EngineSyncResult) {
	p.AddSyncResult(result.SyncResults...)
	p.SyncError = result.SyncError
}

// Fail fails the EngineSyncResult with an error occurred in Sync() and Teardown() itself
func (p *EngineSyncResult) Fail(err error) {
	p.SyncError = err
}

// Error returns an error summarizing all the errors in EngineSyncResult
func (p *EngineSyncResult) Error() error {
	errlist := []error{}
	if p.SyncError != nil {
		errlist = append(errlist, fmt.Errorf("failed to Sync: %v", p.SyncError))
	}
	for _, result := range p.SyncResults {
		if result.Error != nil {
			errlist = append(errlist, fmt.Errorf("failed to %q for %q with %v: %q", result.Action, result.Target,
				result.Error, result.Message))
		}
	}
	return utilerrors.NewAggregate(errlist)
}
