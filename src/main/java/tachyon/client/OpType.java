package tachyon.client;

public enum OpType {
  // Read the file and cache it.
//  READ_CACHE(1),
  // Read the file and but do not cache it explictly.
  READ_NO_CACHE(2),
  // Write the file and cache it.
  // TODO This is for local testing only for now.
  WRITE_CACHE_NO_THROUGH(1 << 4),
  // Write the file synchronously, cache it,
  WRITE_CACHE_THROUGH(1 << 4 + 1),
  // Write the file synchronously, no cache.
  WRITE_THROUGH_NO_CACHE(1 << 4 + 2);

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

  public boolean isWriteThrough() {
    return (mValue == WRITE_CACHE_THROUGH.mValue) || (mValue == WRITE_THROUGH_NO_CACHE.mValue);
  }
  
  public boolean isWriteCache() {
    return (mValue == WRITE_CACHE_NO_THROUGH.mValue) || (mValue == WRITE_CACHE_THROUGH.mValue);
  }
}