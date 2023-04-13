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
}
