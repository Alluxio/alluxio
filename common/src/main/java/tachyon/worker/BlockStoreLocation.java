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

package tachyon.worker;

/**
 * Where to store a block within a block store. Currently, this is a wrapper on an integer
 * representing the tier to put this block.
 */
public class BlockStoreLocation {
  private static final int ANY_TIER = -1;
  private static final int ANY_DIR = -1;
  private final int mTierAlias;
  private final int mDirIndex;

  public static BlockStoreLocation anyTier() {
    return new BlockStoreLocation(ANY_TIER, ANY_DIR);
  }

  public static BlockStoreLocation anyDirInTier(int tierAlias) {
    return new BlockStoreLocation(tierAlias, ANY_DIR);
  }

  public BlockStoreLocation(int tierAlias, int dirIndex) {
    mTierAlias = tierAlias;
    mDirIndex = dirIndex;
  }

  // A helper function to derive StorageDirId from a BlockLocation.
  // TODO: remove this method when master also understands BlockLocation
  public long getStorageDirId() {
    // TODO: level is alias - 1 as alias of MEM is 1, SSD is 2 and HDD is 3.
    int level = mTierAlias - 1;
    // Calculation copied from {@link StorageDirId.getStorageDirId}
    return (level << 24) + (mTierAlias << 16) + mDirIndex;
  }

  public int tierAlias() {
    return mTierAlias;
  }

  public int tierLevel() {
    return mTierAlias - 1;
  }

  public int dir() {
    return mDirIndex;
  }

  /**
   * Whether this location belongs to the specific location
   *
   * a location A belongs to B either when A.equals(B) or tier and dir of A are all in the range of
   * B
   *
   * @param location the target BlockStoreLocation
   * @return true when this BlockStoreLocation belongs to the target, otherwise false
   */
  public boolean belongTo(BlockStoreLocation location) {
    if (equals(location)) {
      return true;
    }
    boolean tierInRange =
        (tierAlias() == location.tierAlias()) || (location.tierAlias() == ANY_TIER);
    boolean dirInRange = (dir() == location.dir()) || (location.dir() == ANY_DIR);
    return tierInRange && dirInRange;
  }

  @Override
  public String toString() {
    String result = "";
    if (mDirIndex == ANY_DIR) {
      result += "any dir";
    } else {
      result += "dir " + mDirIndex;
    }

    if (mTierAlias == ANY_TIER) {
      result += ", any tier";
    } else {
      result += ", tierAlias " + mTierAlias;
    }
    return result;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof BlockStoreLocation
        && ((BlockStoreLocation) object).tierAlias() == tierAlias()
        && ((BlockStoreLocation) object).dir() == dir()) {
      return true;
    } else {
      return false;
    }
  }
}
