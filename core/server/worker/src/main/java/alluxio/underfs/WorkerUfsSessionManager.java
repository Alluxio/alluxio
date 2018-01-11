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

package alluxio.underfs;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The default implementation of UfsSessionManager to manage the ufs sessions by different worker
 * services.
 */
@ThreadSafe
public final class WorkerUfsSessionManager extends AbstractUfsSessionManager {
  /**
   * Constructs an instance of {@link WorkerUfsSessionManager}.
   *
   * @param ufsManager the worker ufs manager
   */
  public WorkerUfsSessionManager(UfsManager ufsManager) {
    super(ufsManager);
  }
}
