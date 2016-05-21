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

package alluxio.worker.block;

import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Where to store a block within a block store. Currently, this is a wrapper on an integer
 * representing the tier to put this block.
 */
@ThreadSafe
public final class BlockStoreLocation {

  /** Special value to indicate any tier. */
  private static final String ANY_TIER = "";

  /** Special value to indicate any dir. */
  private static final int ANY_DIR = -1;

  /** Alias of the storage tier. */
  private final String mTierAlias;

  /** Index of the directory in its tier, 0 indexed. */
  private final int mDirIndex;

  /**
   * Convenience method to return the block store location representing any dir in any tier.
   *
   * @return a BlockStoreLocation of any dir in any tier
   */
  public static BlockStoreLocation anyTier() {
    return new BlockStoreLocation(ANY_TIER, ANY_DIR);
  }

  /**
   * Convenience method to return the block store location representing any dir in the tier.
   *
   * @param tierAlias The alias of the tier this returned block store location will represent
   * @return a BlockStoreLocation of any dir in the specified tier
   */
  public static BlockStoreLocation anyDirInTier(String tierAlias) {
    return new BlockStoreLocation(tierAlias, ANY_DIR);
  }

  /**
   * Creates a new instance of {@link BlockStoreLocation}.
   *
   * @param tierAlias the tier alias to use
   * @param dirIndex the directory index to use
   */
  public BlockStoreLocation(String tierAlias, int dirIndex) {
    mTierAlias = tierAlias;
    mDirIndex = dirIndex;
  }

  /**
   * Gets the storage tier alias of the location.
   *
   * @return the tier alias of the location
   */
  public String tierAlias() {
    return mTierAlias;
  }

  /**
   * Gets the directory index of the location.
   *
   * @return the directory index of the location, {@link #ANY_DIR} for any directory
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
  public boolean belongsTo(BlockStoreLocation location) {
    boolean tierInRange =
        tierAlias().equals(location.tierAlias()) || location.tierAlias().equals(ANY_TIER);
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

    if (mTierAlias.equals(ANY_TIER)) {
      result.append(", any tier");
    } else {
      result.append(", tierAlias ").append(mTierAlias);
    }
    return result.toString();
  }

  /**
   * Compares to a specific object.
   *
   * @param o the object to compare
   * @return true if object is also {@link BlockStoreLocation} and represents the same tier and dir
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BlockStoreLocation)) {
      return false;
    }
    BlockStoreLocation that = (BlockStoreLocation) o;
    return mTierAlias.equals(that.mTierAlias) && mDirIndex == that.mDirIndex;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[] {mTierAlias, mDirIndex});
  }
}
