package tachyon;

public enum LogType {
  Undefined(0),
  CheckpointInfo(1),
  InodeFile(2),
  InodeFolder(3),
  InodeRawTable(4),
  Dependency(5);

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
