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

package alluxio.job;

import alluxio.underfs.UfsManager;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The context is used by job to access master-side resources and information.
 */
@ThreadSafe
public final class JobMasterContext {
  private final long mJobId;
  private final UfsManager mUfsManager;

  /**
   * @param jobId the job id
   * @param ufsManager the UFS manager
   */
  public JobMasterContext(long jobId, UfsManager ufsManager) {
    mJobId = jobId;
    mUfsManager = ufsManager;
  }

  /**
   * @return the job id
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * @return the UFS manager
   */
  public UfsManager getUfsManager() {
    return mUfsManager;
  }
}
