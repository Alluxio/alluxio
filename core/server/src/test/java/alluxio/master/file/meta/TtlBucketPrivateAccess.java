/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.meta;

import org.powermock.reflect.Whitebox;

/**
 * Class which provides access to private state of {@link TtlBucket}.
 */
public final class TtlBucketPrivateAccess {

  /**
   * Sets the {@link TtlBucket#sTtlIntervalMs} variable for testing.
   *
   * @param intervalMs the interval in milliseconds
   */
  public static void setTtlIntervalMs(long intervalMs) {
    Whitebox.setInternalState(TtlBucket.class, "sTtlIntervalMs", intervalMs);
  }
}
