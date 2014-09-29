package tachyon;

/**
 * Used to identify StorageDir in hierarchy store.
 */
public class StorageDirId {
  static final long UNKNOWN = -1;

  /**
   * Generate StorageDirId from given information
   * 
   * @param level storage level of the StorageDir
   * @param aliasValue storage level alias value of the StorageDir
   * @param dirIndex index of the StorageDir in StorageTier which contains it
   * @return StorageDirId generated
   */
  public static long getStorageDirId(int level, int aliasValue, int dirIndex) {
    return (level << 24) + (aliasValue << 16) + dirIndex;
  }

  /**
   * Get index of the StorageDir in the StorageTier which contains it
   * 
   * @param storageDirId Id of the StorageDir
   * @return
   */
  public static int getStorageDirIndex(long storageDirId) {
    return (int) storageDirId & 0x00ff;
  }

  /**
   * Get storage level of StorageTier which contains the StorageDir
   * 
   * @param storageDirId Id of the StorageDir
   * @return storage level of the StorageTier
   */
  public static int getStorageLevel(long storageDirId) {
    return ((int) storageDirId >> 24) & 0x0f;
  }

  /**
   * Get StorageLevelAlias value from StorageDirId
   * 
   * @param storageDirId Id of the StorageDir
   * @return value of StorageLevelAlias
   */
  public static int getStorageLevelAliasValue(long storageDirId) {
    return ((int) storageDirId >> 16) & 0x0f;
  }

  /**
   * Check whether StorageDirId is unknown
   * 
   * @param storageDirId Id of the StorageDir
   * @return true if StorageDirId is unknown, false otherwise.
   */
  public static boolean isUnknown(long storageDirId) {
    return storageDirId == UNKNOWN;
  }

  /**
   * Get unknown value of StorageDirId
   * 
   * @return unknown value of StorageDirId
   */
  public static long unknownId() {
    return UNKNOWN;
  }

  private StorageDirId() {}
}
