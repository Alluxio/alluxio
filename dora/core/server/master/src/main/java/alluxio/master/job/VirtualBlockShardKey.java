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

import static java.lang.String.format;

import alluxio.common.ShardKey;

/**
 * The key for accessing the hash ring.
 * There's two types of usage for this key:
 * 1. metadata operation: the partition index is 0
 * 2. data operation: the partition index is offset / virtual block size
 *
 */
public final class VirtualBlockShardKey implements ShardKey {
  private final String mUFSPath;
  private final int mVirtualBlockIndex;

  /**
   * constructor for VirtualBlockShardKey.
   *
   * @param ufsPath           the ufs path
   * @param virtualBlockIndex the virtual block index
   */
  public VirtualBlockShardKey(String ufsPath, int virtualBlockIndex) {
    mUFSPath = ufsPath;
    mVirtualBlockIndex = virtualBlockIndex;
  }

  @Override
  public String asString() {
    // use the same format as ConsistentHashProvider,
    // we can't distinguish the two index, but it's fine
    return format("%s%d", mUFSPath, mVirtualBlockIndex);
  }
}
