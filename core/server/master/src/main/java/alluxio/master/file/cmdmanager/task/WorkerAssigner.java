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

import alluxio.master.file.cmdmanager.command.FsCmdConfig;

import java.util.Set;

/**
 * Interface for selecting workers and assigning tasks to workers.
 * @param <T> type of assigned tasks
 */
public interface WorkerAssigner<T> {
  /**
   * Get address information of workers which would run the command.
   * @return a set of worker addresses
   */
  Set<ExecutionWorkerInfo> getWorkers();

  /**
   * Create and assign tasks to workers for a given config.
   * @param config filesystem command config
   * @param info worker info set
   * @return task assignment for workers
   */
  T createCommandAssignment(FsCmdConfig config, Set<ExecutionWorkerInfo> info);
}
