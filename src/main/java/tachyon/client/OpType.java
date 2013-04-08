package tachyon.client;

import java.io.IOException;

public enum OpType {
  // Read the file and but do not cache it explicitly.
  READ_NO_CACHE(1),
  // Read the file and cache it.
  READ_TRY_CACHE(2),
  // Write the file and must cache it on the client machine.
  // TODO This is for local testing only for now.
  WRITE_CACHE(1 << 4),
  // Write the file synchronously, must cache it,
  WRITE_CACHE_THROUGH(1 << 4 + 1),
  // Write the file synchronously, no cache.
  WRITE_THROUGH(1 << 4 + 2);

  private final int mValue;

  private OpType(int value) {
    mValue = value;
  }

  public boolean isRead() {
    return (mValue >> 4) == 0;
  }

  public boolean isReadTryCache() {
    return mValue == READ_TRY_CACHE.mValue;
  }

  public boolean isWrite() {
    return !isRead();
  }

  public boolean isWriteThrough() {
    return (mValue == WRITE_CACHE_THROUGH.mValue) || (mValue == WRITE_THROUGH.mValue);
  }

  public boolean isWriteCache() {
    return (mValue == WRITE_CACHE.mValue) || (mValue == WRITE_CACHE_THROUGH.mValue);
  }

  public static OpType getOpType(String op) throws IOException {
    if (op.equals("READ_NO_CACHE")) {
      return READ_NO_CACHE;
    } else if (op.equals("READ_TRY_CACHE")) {
      return READ_TRY_CACHE;
    } else if (op.equals("WRITE_CACHE")) {
      return WRITE_CACHE;
    } else if (op.equals("WRITE_CACHE_THROUGH")) {
      return WRITE_CACHE_THROUGH;
    } else if (op.equals("WRITE_THROUGH")) {
      return WRITE_THROUGH;
    }

    throw new IOException("Unknown OpType : " + op);
  }
}