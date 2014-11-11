package tachyon.worker.hierarchy;

/**
 * Request space from current StorageTier. It returns the affordable StorageDir according to
 * AllocateStrategy, null if no enough space available on StorageDirs.
 */
public interface AllocateStrategy {
  /**
   * Check whether it is possible to get enough space by evicting some blocks
   * 
   * @param storageDirs candidates of StorageDirs which space will be allocated in
   * @param requestSize size to request in bytes
   * @return true if possible, false otherwise
   */
  public boolean fitInPossible(StorageDir[] storageDirs, long requestSizeBytes);

  /**
   * Allocate space on StorageDirs
   * 
   * @param storageDirs candidates of StorageDirs that space will be allocated in
   * @param userId id of user
   * @param requestSizeBytes size to request in bytes
   * @return StorageDir assigned, null if no StorageDir is assigned
   */
  public StorageDir getStorageDir(StorageDir[] storageDirs, long userId, long requestSizeBytes);
}
