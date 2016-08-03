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

package alluxio.master;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class for aggregating state which should be shared across masters.
 */
@ThreadSafe
public final class MasterContext {
  /**
   * A {@link MasterSource} for collecting master metrics.
   */
  private final MasterSource mMasterSource;

  /**
   * @param masterSource the {@link MasterSource} for this {@link MasterContext}
   */
  public MasterContext(MasterSource masterSource) {
    mMasterSource = masterSource;
  }

  /**
   * @return the {@link MasterSource} for the master process
   */
  public MasterSource getMasterSource() {
    return mMasterSource;
  }
}
