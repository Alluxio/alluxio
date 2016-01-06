/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.web;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import tachyon.thrift.BlockLocation;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.WorkerNetAddress;

public final class UIFileBlockInfo {
  private final List<String> mLocations = new ArrayList<String>();
  private final Set<String> mTierAliases = new HashSet<String>();
  private final long mId;
  private final long mBlockLength;
  private final long mLastAccessTimeMs;

  /**
   * Creates a new instance of {@link UIFileBlockInfo}.
   *
   * @param fileBlockInfo underlying {@link FileBlockInfo}
   */
  public UIFileBlockInfo(FileBlockInfo fileBlockInfo) {
    Preconditions.checkNotNull(fileBlockInfo);
    mId = fileBlockInfo.getBlockInfo().getBlockId();
    mBlockLength = fileBlockInfo.getBlockInfo().getLength();
    mLastAccessTimeMs = -1;
    addLocations(fileBlockInfo);
    for (BlockLocation location : fileBlockInfo.getBlockInfo().getLocations()) {
      mTierAliases.add(location.getTierAlias());
    }
  }

  /**
   * Creates a new instance of {@link UIFileBlockInfo}.
   *
   * @param blockId the block id
   * @param blockLength the block length
   * @param blockLastAccessTimeMs the block last access time in milliseconds
   * @param tierAlias the tier alias of the block
   */
  public UIFileBlockInfo(long blockId, long blockLength, long blockLastAccessTimeMs,
      String tierAlias) {
    mId = blockId;
    mBlockLength = blockLength;
    mLastAccessTimeMs = blockLastAccessTimeMs;
    mTierAliases.add(tierAlias);
  }

  private void addLocations(FileBlockInfo fileBlockInfo) {
    Set<String> locations = Sets.newHashSet();
    // add tachyon locations
    for (BlockLocation location : fileBlockInfo.getBlockInfo().getLocations()) {
      locations.add(location.getWorkerAddress().getHost());
    }
    // add underFS locations
    for (WorkerNetAddress address : fileBlockInfo.getUfsLocations()) {
      locations.add(address.getHost());
    }
    mLocations.addAll(locations);
  }

  /**
   * @return true if the block is in the given tier alias in some worker, false otherwise
   */
  public boolean isInTier(String tierAlias) {
    return mTierAliases.contains(tierAlias);
  }

  /**
   * @return the block length
   */
  public long getBlockLength() {
    return mBlockLength;
  }

  /**
   * @return the block id
   */
  public long getID() {
    return mId;
  }

  /**
   * @return the block last access time in milliseconds
   */
  public long getLastAccessTimeMs() {
    return mLastAccessTimeMs;
  }

  /**
   * @return the block locations
   */
  public List<String> getLocations() {
    return mLocations;
  }
}
