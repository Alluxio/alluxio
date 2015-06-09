package tachyon.worker;

/**
 * Where to store a block within a block store. Currently, this is a wrapper on an integer
 * representing the tier to put this block.
 */
public class BlockStoreLocation {
  private static final int sAnyTier = -1;
  private static final int sAnyDir = -2;
  private final int mTier;
  private final int mDir;

  public static BlockStoreLocation anyTier() {
    return new BlockStoreLocation(sAnyTier, sAnyDir);
  }

  public static BlockStoreLocation anyDirInTier(int tier) {
    return new BlockStoreLocation(tier, sAnyDir);
  }

  public BlockStoreLocation(int tier, int dir) {
    mTier = tier;
    mDir = dir;
  }

  public int tier() {
    return mTier;
  }

  public int dir() {
    return mDir;
  }

  @Override
  public String toString() {
    String result = "";
    if (mDir == sAnyDir) {
      result += "any dir";
    } else {
      result += "dir " + mDir;
    }

    if (mTier == sAnyTier) {
      result += ", any tier"
    } else {
      result += ", tier " + mTier;
    }
    return result;
  }
}
