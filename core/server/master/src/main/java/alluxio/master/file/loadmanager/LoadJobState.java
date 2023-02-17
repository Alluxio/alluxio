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

package alluxio.master.file.loadmanager;

import alluxio.proto.journal.Job;

/**
 * Load status.
 */
public enum LoadJobState
{
  LOADING,
  VERIFYING,
  STOPPED,
  SUCCEEDED,
  FAILED;

  /**
   * Convert LoadStatus to PJobStatus.
   *
   * @param state load job state
   * @return the corresponding PJobStatus
   */
  public static Job.PJobState toProto(LoadJobState state)
  {
    switch (state) {
      case LOADING:
      case VERIFYING:
        return Job.PJobState.CREATED;
      case STOPPED:
        return Job.PJobState.STOPPED;
      case SUCCEEDED:
        return Job.PJobState.SUCCEEDED;
      case FAILED:
        return Job.PJobState.FAILED;
      default:
        throw new IllegalArgumentException(String.format("Unknown state %s", state));
    }
  }

  /**
   * Convert PJobStatus to LoadStatus.
   *
   * @param jobStatus protobuf job status
   * @return the corresponding LoadStatus
   */
  public static LoadJobState fromProto(Job.PJobState jobStatus)
  {
    switch (jobStatus) {
      case CREATED:
        return LOADING;
      case STOPPED:
        return STOPPED;
      case SUCCEEDED:
        return SUCCEEDED;
      case FAILED:
        return FAILED;
      default:
        throw new IllegalArgumentException(String.format("Unknown job status %s", jobStatus));
    }
  }
}
