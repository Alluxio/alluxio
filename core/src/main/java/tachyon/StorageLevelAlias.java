package tachyon;

import java.io.IOException;

/**
 * Different storage level alias for StorageTier.
 */
public enum StorageLevelAlias {

  /**
   * Data is stored in memory
   */
  MEM(1),
  /**
   * Data is stored on SSD
   */
  SSD(2),
  /**
   * Data is stored on HDD
   */
  HDD(3);

  /**
   * Parse the storage level
   * 
   * @param storageLevel the String format of the storage level
   * @return alias of storage level
   * @throws IOException
   */
  public static StorageLevelAlias getStorageLevel(String storageLevel) throws IOException {
    if (storageLevel == null) {
      throw new NullPointerException();
    }
    if (storageLevel.toUpperCase().equals("MEM")) {
      return MEM;
    } else if (storageLevel.toUpperCase().equals("SSD")) {
      return SSD;
    } else if (storageLevel.toUpperCase().equals("HDD")) {
      return HDD;
    }

    throw new IOException("Unknown StorageLevel : " + storageLevel);
  }

  private int VALUE;

  private StorageLevelAlias(int value) {
    this.VALUE = value;
  }

  /**
   * Get value of the storage level alias
   * 
   * @return value of the storage level alias
   */
  public int getValue() {
    return VALUE;
  }
}
