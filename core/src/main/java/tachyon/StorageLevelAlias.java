package tachyon;

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

  private int mValue;

  private StorageLevelAlias(int value) {
    this.mValue = value;
  }

  /**
   * Get value of the storage level alias
   * 
   * @return value of the storage level alias
   */
  public int getValue() {
    return mValue;
  }
}
