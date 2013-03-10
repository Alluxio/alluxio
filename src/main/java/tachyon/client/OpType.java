package tachyon.client;

public enum OpType {
  // Read the file and cache it.
  READ_CACHE(1),
  // Write the file and cache it.
  WRITE_CACHE(1 << 4),
  // Write the file synchronously, cache it,
  WRITE_CACHE_THROUGH(1 << 4 + 1);
  // Write the file synchronously, no cache.
  //  WRITE_THROUGH(1 << 4 + 2);

  private final int mValue;

  private OpType(int value) {
    mValue = value;
  }

  public boolean isRead() {
    return (mValue >> 4) == 0;
  }

  public boolean isWrite() {
    return !isRead();
  }

  public boolean isSyncWrite() {
    return (mValue == WRITE_CACHE_THROUGH.mValue);
  }
}