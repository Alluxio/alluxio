package tachyon.worker.hierarchy;

/**
 * Used for recording block information on worker side.
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
   * Get Id of the block
   * 
   * @return Id of the block
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
   * Get the StorageDir which contains the block
   * 
   * @return StorageDir which contains the block
   */
  public StorageDir getStorageDir() {
    return mDir;
  }
}
