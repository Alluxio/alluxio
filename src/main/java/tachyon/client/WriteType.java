package tachyon.client;

import java.io.IOException;

/**
 * Different write types for a TachyonFile. 
 */
public enum WriteType {
  // Write the file and must cache it on the client machine.
  // TODO This is for local testing only for now.
  CACHE(1),
  // Write the file synchronously, must cache it,
  CACHE_THROUGH(2),
  // Write the file synchronously, no cache.
  THROUGH(3);

  private final int mValue;

  private WriteType(int value) {
    mValue = value;
  }

  public int getValue() {
    return mValue;
  }

  public boolean isThrough() {
    return (mValue == CACHE_THROUGH.mValue) || (mValue == THROUGH.mValue);
  }

  public boolean isCache() {
    return (mValue == CACHE.mValue) || (mValue == CACHE_THROUGH.mValue);
  }

  public boolean isMustCache() {
    return mValue == CACHE.mValue;
  }

  public static WriteType getOpType(String op) throws IOException {
    if (op.equals("CACHE")) {
      return CACHE;
    } else if (op.equals("CACHE_THROUGH")) {
      return CACHE_THROUGH;
    } else if (op.equals("THROUGH")) {
      return THROUGH;
    }

    throw new IOException("Unknown WriteType : " + op);
  }
}
