package tachyon;

/**
 * Different class types in Tachyon master write ahead log or checkpoint.
 */
public enum LogType {
  Undefined(0),
  CheckpointInfo(1),
  InodeFile(2),
  InodeFolder(3),
  InodeRawTable(4);

  private final int value;

  private LogType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value.
   */
  public int getValue() {
    return value;
  }
}
