package tachyon.client;

import java.io.IOException;

/**
 * Different write types for a TachyonFile. 
 */
public enum WriteType {
  // Write the file and must cache it.
  CACHE(1),
  // Write the file and try to cache it.
  TRY_CACHE(2),
  // Write the file synchronously to the under fs, and also try to cache it,
  CACHE_THROUGH(3),
  // Write the file synchronously to the under fs, no cache.
  THROUGH(4),
  // Write the file and must cache it. [just like CACHE(1)]  To support OLDER style in the wiki doc.
  WRITE_CACHE(5);

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
    return (mValue == CACHE.mValue) 
        || (mValue == CACHE_THROUGH.mValue)
        || (mValue == TRY_CACHE.mValue)
        || (mValue == WRITE_CACHE.mValue);
  }

  public boolean isMustCache() {
    return (mValue == CACHE.mValue)
        || (mValue == WRITE_CACHE.mValue);
  }

  public static WriteType getOpType(String op) throws IOException {
    if (op.equals("CACHE")) {
      return CACHE;
    } else if (op.equals("CACHE_THROUGH")) {
      return CACHE_THROUGH;
    } else if (op.equals("THROUGH")) {
      return THROUGH;
    } else if (op.equals("WRITE_CACHE")) {
      return WRITE_CACHE;
    }

    throw new IOException("Unknown WriteType : " + op);
  }
}
