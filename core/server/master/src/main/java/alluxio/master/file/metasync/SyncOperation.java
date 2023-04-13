package alluxio.master.file.metasync;

public enum SyncOperation {
  NOOP(0),
  CREATE(1),
  DELETE(2),
  RECREATE(3),
  UPDATE(4),
  SKIPPED_DUE_TO_CONCURRENT_MODIFICATION(5),
  SKIPPED_ON_MOUNT_POINT(6);

  private final int mValue;

  public int getValue() {
    return mValue;
  }

  SyncOperation(int value) {
    mValue = value;
  }

  public static SyncOperation fromInteger(int value) {
    switch (value) {
      case 0:
        return NOOP;
      case 1:
        return CREATE;
      case 2:
        return DELETE;
      case 3:
        return RECREATE;
      case 4:
        return UPDATE;
      case 5:
        return SKIPPED_DUE_TO_CONCURRENT_MODIFICATION;
      case 6:
        return SKIPPED_ON_MOUNT_POINT;
      default:
        throw new IllegalArgumentException("Invalid SyncOperation value: " + value);
    }
  }
}
