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

package alluxio.worker;

import alluxio.underfs.UfsManager;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Interface for factory of {@link Worker}.
 */
@ThreadSafe
public interface WorkerFactory {
  /**
   * @return whether the worker is enabled
   */
  boolean isEnabled();

  /**
   * Factory method to create a new worker instance.
   *
   * @param registry the worker registry
   * @param ufsManager the UFS manager
   * @return a new {@link Worker} instance
   */
  Worker create(WorkerRegistry registry, UfsManager ufsManager);
}
