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

package alluxio.client;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Specifies the type of data interaction with Alluxio.
 * <ul>
 * <li>For a write operation, this determines whether the data will be written into Alluxio storage.
 * Metadata will always be updated in Alluxio space.</li>
 * <li>For a read operation, this determines whether fully read blocks will be stored in Alluxio
 * storage.</li>
 * </ul>
 */
@ThreadSafe
public enum AlluxioStorageType {
  /** Put the data reading or writing in Alluxio storage. */
  STORE(1),

  /** Do not put data to Alluxio. */
  NO_STORE(2),

  /**
   * Same as {@link #STORE} for writes. Will move the data to highest tier before access for reads.
   */
  PROMOTE(3),
  ;

  private final int mValue;

  AlluxioStorageType(int value) {
    mValue = value;
  }

  /**
   * @return whether the data should be put in Alluxio storage
   */
  public boolean isStore() {
    return mValue == STORE.mValue || mValue == PROMOTE.mValue;
  }

  /**
   * @return whether the data should be promoted to the highest tier
   */
  public boolean isPromote() {
    return mValue == PROMOTE.mValue;
  }
}
