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

package alluxio.master.file.cmdmanager.command;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Task in terms of block ids.
 */
public class Task {
  private final List<Long> mBlockIds;
  private final long mTaskId;

  private ExecutionStatus mExecutionStatus;

  /**
   * Constructor.
   * @param blockIds list of block ids
   * @param taskId task id
   */
  public Task(List<Long> blockIds, long taskId) {
    mBlockIds = blockIds;
    mTaskId = taskId;
  }

  /**
   * Set ExecutionStatus.
   * @param status ExecutionStatus
   */
  public void setExecutionStatus(ExecutionStatus status) {
    mExecutionStatus = status;
  }

  /**
   * Get task id.
   * @return task id
   */
  public long getTaskId() {
    return mTaskId;
  }
}
