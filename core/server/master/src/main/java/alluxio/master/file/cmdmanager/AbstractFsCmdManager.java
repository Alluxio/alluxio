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

import alluxio.job.wire.Status;
import alluxio.retry.RetryPolicy;

import java.io.Closeable;

/**
 * Interface for managing distributed commands.
 *  @param <T> type of the run-task config category of the fs cmd manager
 *  @param <K> type of the key for looking up the fs cmd manager
 */
public interface AbstractFsCmdManager<T, K> extends Closeable {
  /**
   * Validate and do operations based on its status in the fs cmd manager.
   * @param config the config of the task
   * @return boolean flag on validated true or false
   */
  boolean validate(T config);

  /**
   * Run the fs cmd manager to execute for a given option.
   * @param option the option for an execution
   * @param retryPolicy the retry policy
   */
  void schedule(T option, RetryPolicy retryPolicy);

  /**
   * Check the status from fs cmd manager.
   * @param key the key to look up
   * @return status value
   */
  Status getStatus(K key);

  /**
   * Update to run if the config changes.
   * @param config the config of the task
   */
  void update(T config);

  /**
   * Stop the run task based on id.
   * @param id the id of the task
   */
  void stop(K id);
}
