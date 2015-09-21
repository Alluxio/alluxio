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

package tachyon.worker.block;

import java.util.Arrays;

/**
 * Where to store a block within a block store. Currently, this is a wrapper on an integer
 * representing the tier to put this block.
 */
public final class BlockStoreLocation {
  /** Special value to indicate any tier */
  private static final int ANY_TIER = -1;
  /** Special value to indicate any dir */
  private static final int ANY_DIR = -1;
  /** NOTE: only reason to have level here is to calculate StorageDirId */
  private static final int UNKNOWN_LEVEL = -1;

  /** Tier alias of the location, see {@link tachyon.StorageLevelAlias} */
  private final int mTierAlias;
  /** Tier level of the location, generally alias - 1, this is 0 indexed */
  private final int mTierLevel;
  /** Index of the directory in its tier, 0 indexed */
  private final int mDirIndex;

  /**
   * Convenience method to return the block store location representing any dir in any tier.
   *
   * @return a BlockStoreLocation of any dir in any tier
   */
  public static BlockStoreLocation anyTier() {
    return new BlockStoreLocation(ANY_TIER, UNKNOWN_LEVEL, ANY_DIR);
  }

  /**
   * Convenience method to return the block store location representing any dir in the tier.
   *
   * @param tierAlias The tier this returned block store alias will represent
   * @return a BlockStoreLocation of any dir in the specified tier
   */
  public static BlockStoreLocation anyDirInTier(int tierAlias) {
    return new BlockStoreLocation(tierAlias, UNKNOWN_LEVEL, ANY_DIR);
  }

  public BlockStoreLocation(int tierAlias, int tierLevel, int dirIndex) {
    mTierLevel = tierLevel;
    mTierAlias = tierAlias;
    mDirIndex = dirIndex;
  }

  /**
   * Gets the storage directory id of the location. The first 8 bits are tier level, next 8 are the
   * tier alias and last 16 represent the directory index.
   *
   * @return the storage directory id of the location
   */
  // TODO(gene): Remove this method when master also understands MasterBlockLocation.
  public long getStorageDirId() {
    // Calculation copied from {@link StorageDirId.getStorageDirId}
    return (mTierLevel << 24) + (mTierAlias << 16) + mDirIndex;
  }

  /**
   * Gets the tier alias of the location.
   *
   * @return the tier alias of the location, -1 for any tier
   */
  public int tierAlias() {
    return mTierAlias;
  }

  /**
   * Gets the tier level of the location.
   *
   * @return the tier level of the location, -1 for unknown level
   */
  public int tierLevel() {
    return mTierLevel;
  }

  /**
   * Gets the directory index of the location.
   *
   * @return the directory index of the location, -1 for any directory
   */
  public int dir() {
    return mDirIndex;
  }

  /**
   * Returns whether this location belongs to the specific location.
   *
   * Location A belongs to B when tier and dir of A are all in the range of B respectively.
   *
   * @param location the target BlockStoreLocation
   * @return true when this BlockStoreLocation belongs to the target, otherwise false
   */
  public boolean belongTo(BlockStoreLocation location) {
    boolean tierInRange =
        (tierAlias() == location.tierAlias()) || (location.tierAlias() == ANY_TIER);
    boolean dirInRange = (dir() == location.dir()) || (location.dir() == ANY_DIR);
    return tierInRange && dirInRange;
  }

  /**
   * Converts the location to a human readable form.
   *
   * @return a human readable string representing the information of this block store location
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (mDirIndex == ANY_DIR) {
      result.append("any dir");
    } else {
      result.append("dir ").append(mDirIndex);
    }

    if (mTierAlias == ANY_TIER) {
      result.append(", any tier");
    } else {
      result.append(", tierAlias ").append(mTierAlias);
    }
    return result.toString();
  }

  /**
   * Compares to a specific object.
   *
   * @param object the object to compare
   * @return true if object is also {@link BlockStoreLocation} and represents the same tier and dir.
   */
  @Override
  public boolean equals(Object object) {
    return object instanceof BlockStoreLocation
        && ((BlockStoreLocation) object).tierLevel() == tierLevel()
        && ((BlockStoreLocation) object).tierAlias() == tierAlias()
        && ((BlockStoreLocation) object).dir() == dir();
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[] {mTierLevel, mTierAlias, mDirIndex});
  }
}
