package tachyon.worker;

/**
 * Where to store a block within a block store. Currently, this is a wrapper on an integer
 * representing the tier to put this block.
 */
public class BlockStoreLocation {
  private static final int sAnyTier = -1;
  private final int mTier;

  public static BlockStoreLocation anyTier() {
    return new BlockStoreLocation(sAnyTier);
  }

  public BlockStoreLocation(int tier) {
    mTier = tier;
  }

  public int tier() {
    return mTier;
  }
}
