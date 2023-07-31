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

package alluxio.master.job;

import java.util.OptionalInt;

/**
 * The key for accessing the hash ring .
 */
public final class HashKey {
  private String mUFSPath;
  private final OptionalInt mVirtualBlockIndex;

  /**
   * @return the partition index of the key
   */
  public OptionalInt getVirtualBlockIndex() {
    return mVirtualBlockIndex;
  }

  /**
   * constructor for HashKey.
   *
   * @param ufsPath           the ufs path
   * @param virtualBlockIndex the virtual block index
   */
  public HashKey(String ufsPath, OptionalInt virtualBlockIndex) {
    mUFSPath = ufsPath;
    mVirtualBlockIndex = virtualBlockIndex;
  }

  /**
   * @return the ufs path
   */
  public String getUFSPath() {
    return mUFSPath;
  }

  @Override
  public String toString() {
    if (mVirtualBlockIndex.isPresent() && mVirtualBlockIndex.getAsInt() != 0) {
      return mUFSPath + ":" + mVirtualBlockIndex.getAsInt();
    }
    else {
      return mUFSPath;
    }
  }

  /**
   * @return true if the key represents metadata
   */
  public boolean isMetadata() {
    return !mVirtualBlockIndex.isPresent();
  }
}
