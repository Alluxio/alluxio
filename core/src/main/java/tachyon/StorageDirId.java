package tachyon;

/**
 * Used to identify StorageDir in hierarchy store.
 */
public class StorageDirId {
  static final long UNKNOWN = -1;

  /**
   * Generate StorageDirId from given information
   * 
   * @param level storage level of the StorageTier which contains the StorageDir
   * @param storageLevelaliasValue StorageLevelAlias value of the StorageTier
   * @param dirIndex index of the StorageDir
   * @return StorageDirId generated
   */
  public static long getStorageDirId(int level, int storageLevelAliasValue, int dirIndex) {
    return (level << 24) + (storageLevelAliasValue << 16) + dirIndex;
  }

  /**
   * Get index of the StorageDir in the StorageTier which contains it
   * 
   * @param storageDirId Id of the StorageDir
   * @return index of the StorageDir
   */
  public static int getStorageDirIndex(long storageDirId) {
    return (int) storageDirId & 0x00ff;
  }

  /**
   * Get storage level of StorageTier which contains the StorageDir
   * 
   * @param storageDirId Id of the StorageDir
   * @return storage level of the StorageTier which contains the StorageDir
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
   * Check whether the value of StorageDirId is UNKNOWN
   * 
   * @param storageDirId Id of the StorageDir
   * @return true if StorageDirId is UNKNOWN, false otherwise.
   */
  public static boolean isUnknown(long storageDirId) {
    return storageDirId == UNKNOWN;
  }

  /**
   * Get unknown value of StorageDirId, which indicates the StorageDir is unknown
   * 
   * @return UNKNOWN value of StorageDirId
   */
  public static long unknownId() {
    return UNKNOWN;
  }

  private StorageDirId() {}
}
