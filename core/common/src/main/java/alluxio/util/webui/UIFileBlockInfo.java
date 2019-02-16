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

package alluxio.util.webui;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
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
  private final boolean mIsInHighestTier;

  /**
   * Creates a new instance of {@link alluxio.util.webui.UIFileBlockInfo}.
   *
   * @param fileBlockInfo underlying {@link FileBlockInfo}
   * @param alluxioConfiguration the alluxio configuration
   */
  public UIFileBlockInfo(FileBlockInfo fileBlockInfo, AlluxioConfiguration alluxioConfiguration) {
    Preconditions.checkNotNull(fileBlockInfo, "fileBlockInfo");
    mId = fileBlockInfo.getBlockInfo().getBlockId();
    mBlockLength = fileBlockInfo.getBlockInfo().getLength();
    mLastAccessTimeMs = -1;
    addLocations(fileBlockInfo);
    for (BlockLocation location : fileBlockInfo.getBlockInfo().getLocations()) {
      mTierAliases.add(location.getTierAlias());
    }
    mIsInHighestTier = mTierAliases
        .contains(alluxioConfiguration.get(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS));
  }

  /**
   * Creates a new instance of {@link alluxio.util.webui.UIFileBlockInfo}.
   *
   * @param blockId the block id
   * @param blockLength the block length
   * @param blockLastAccessTimeMs the block last access time in milliseconds
   * @param tierAlias the tier alias of the block
   * @param alluxioConfiguration the alluxio configuration
   */
  public UIFileBlockInfo(long blockId, long blockLength, long blockLastAccessTimeMs,
      String tierAlias, AlluxioConfiguration alluxioConfiguration) {
    mId = blockId;
    mBlockLength = blockLength;
    mLastAccessTimeMs = blockLastAccessTimeMs;
    mTierAliases.add(tierAlias);
    mIsInHighestTier = mTierAliases
        .contains(alluxioConfiguration.get(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS));
  }

  private void addLocations(FileBlockInfo fileBlockInfo) {
    Set<String> locations = new HashSet<>();
    // add alluxio locations
    for (BlockLocation location : fileBlockInfo.getBlockInfo().getLocations()) {
      locations.add(location.getWorkerAddress().getHost());
    }
    // add underFS locations
    for (String location : fileBlockInfo.getUfsLocations()) {
      locations.add(HostAndPort.fromString(location).getHost());
    }
    mLocations.addAll(locations);
  }

  /**
   * Is in tier boolean.
   *
   * @param tierAlias the alias for the tier
   * @return true if the block is in the given tier alias in some worker, false otherwise
   */
  public boolean isInTier(String tierAlias) {
    return mTierAliases.contains(tierAlias);
  }

  /**
   * Gets block length.
   *
   * @return the block length
   */
  public long getBlockLength() {
    return mBlockLength;
  }

  /**
   * Gets id.
   *
   * @return the block id
   */
  public long getID() {
    return mId;
  }

  /**
   * Gets last access time ms.
   *
   * @return the block last access time in milliseconds
   */
  public long getLastAccessTimeMs() {
    return mLastAccessTimeMs;
  }

  /**
   * Gets locations.
   *
   * @return the block locations
   */
  public List<String> getLocations() {
    return mLocations;
  }

  /**
   * Gets whether the block is in the highest tier alias.
   *
   * @return true if it's in the highest tier alias
   */
  public boolean getIsInHighestTier() {
    return mIsInHighestTier;
  }
}
