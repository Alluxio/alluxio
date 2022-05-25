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

package alluxio.master.file.cmdmanager.task;

/**
 * Interface for distributed command block tasks.
 */
public interface BlockTask {
  /**
   * Run a command block task.
   */
  void runBlockTask() throws InterruptedException;

  /**
   * Get name for the task.
   * @return task name
   */
  String getName();

  /**
   * Stop the BlockTask.
   */
  void stop();

  /**
   * Get task ID.
   * @return ID value
   */
  long getId();
}
