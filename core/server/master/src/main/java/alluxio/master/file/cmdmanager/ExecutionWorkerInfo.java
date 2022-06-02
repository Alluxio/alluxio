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

package alluxio.master.file.cmdmanager;

import alluxio.master.file.cmdmanager.command.ExecutionStatus;
import alluxio.master.file.cmdmanager.command.Task;

/**
 * Interface for assigned worker info.
 */
public class ExecutionWorkerInfo {
  /**
   * Get worker capacity status, whether under pressure.
   * @return boolean
   */
  public boolean getCapacityStatus()  {
    return true;
  }

  /**
   * Execute a task.
   * @param task task
   * @return future of status
   */
  public ExecutionStatus execute(Task task) {
    return null;
  }
}
