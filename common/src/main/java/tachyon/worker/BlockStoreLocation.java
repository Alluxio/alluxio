package tachyon.worker;

/**
 * Where to store a block within a block store. Currently, this is a wrapper on an integer
 * representing the tier to put this block.
 */
public class BlockStoreLocation {
  private static final int sAnyTier = -1;
  private static final int sAnyDir = -1;
  private final int mTierAlias;
  private final int mDirIndex;

  public static BlockStoreLocation anyTier() {
    return new BlockStoreLocation(sAnyTier, sAnyDir);
  }

  public static BlockStoreLocation anyDirInTier(int tierAlias) {
    return new BlockStoreLocation(tierAlias, sAnyDir);
  }

  public BlockStoreLocation(int tierAlias, int dirIndex) {
    mTierAlias = tierAlias;
    mDirIndex = dirIndex;
  }

  // A helper function to derive StorageDirId from a BlockLocation.
  // TODO: remove this method when master also understands BlockLocation
  public long getStorageDirId() {
    // TODO: double check if mTierAlias is really the level
    return (mTierAlias << 24) + (mTierAlias << 16) + mDirIndex;
  }

  public int tier() {
    return mTierAlias;
  }

  public int dir() {
    return mDirIndex;
  }

  @Override
  public String toString() {
    String result = "";
    if (mDirIndex == sAnyDir) {
      result += "any dir";
    } else {
      result += "dir " + mDirIndex;
    }

    if (mTierAlias == sAnyTier) {
      result += ", any tier";
    } else {
      result += ", tier " + mTierAlias;
    }
    return result;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof BlockStoreLocation &&
        ((BlockStoreLocation) object).tier() == tier() &&
        ((BlockStoreLocation) object).dir() == dir()) {
      return true;
    } else {
      return false;
    }
  }
}
