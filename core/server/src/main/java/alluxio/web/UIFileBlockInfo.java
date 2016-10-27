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

package alluxio.web;

import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Contains information for displaying a file block in the UI.
 */
@ThreadSafe
public final class UIFileBlockInfo {
  private final List<String> mLocations = new ArrayList<>();
  private final Set<String> mTierAliases = new HashSet<>();
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
    Set<String> locations = new HashSet<>();
    // add alluxio locations
    for (BlockLocation location : fileBlockInfo.getBlockInfo().getLocations()) {
      locations.add(location.getWorkerAddress().getHost());
    }
    // add underFS locations
    for (String location : fileBlockInfo.getUfsLocations()) {
      locations.add(HostAndPort.fromString(location).getHostText());
    }
    mLocations.addAll(locations);
  }

  /**
   * @param tierAlias the alias for the tier
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
