package tachyon;

/**
 * Different Inodes in Tachyon Filesystem's master.
 */
public enum InodeType {
  File(1),
  Folder(2),
  RawTable(3);

  private final int mValue;

  private InodeType(int value) {
    mValue = value;
  }

  /**
   * Get the integer value of this enum value.
   */
  public int getValue() {
    return mValue;
  }
}