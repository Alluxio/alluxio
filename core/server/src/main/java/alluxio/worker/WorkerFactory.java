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

import java.util.List;

/**
 * Interface for factory of {@link Worker}.
 */
public interface WorkerFactory {
  /**
   * Factory method to create a new worker instance.
   *
   * @param workers a list of other workers that this worker depends on
   * @return a new {@link Worker} instance or null if failed to launch the worker
   */
  Worker create(List<? extends Worker> workers);
}
