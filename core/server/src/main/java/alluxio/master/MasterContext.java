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
 * A singleton for storing shared master state.
 */
@ThreadSafe
public final class MasterContext {
  private MasterContext() {} // to prevent initialization

  /**
   * The {@link MasterSource} for collecting master metrics.
   */
  private static MasterSource sMasterSource = new MasterSource();

  /**
   * @return the {@link MasterSource} for the master process
   */
  public static MasterSource getMasterSource() {
    return sMasterSource;
  }

  /**
   * Resets the master context.
   */
  public static void reset() {
    sMasterSource = new MasterSource();
  }
}
