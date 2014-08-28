package tachyon.worker.hierarchy;

/**
 * Used for recording block information that will be used in block eviction.
 */
public final class BlockInfo {
  private final StorageDir mDir;
  private final long mBlockId;
  private final long mBlockSize;

  public BlockInfo(StorageDir storageDir, long blockId, long blockSize) {
    mDir = storageDir;
    mBlockId = blockId;
    mBlockSize = blockSize;
  }

  /**
   * Get id of the block
   * 
   * @return id of the block
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * Get size of the block
   * 
   * @return size of the block
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * Get the storage dir which contains the block
   * 
   * @return index of the storage dir
   */
  public StorageDir getStorageDir() {
    return mDir;
  }
}
