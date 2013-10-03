package tachyon.client;

import java.io.IOException;

/**
 * Different read types for a TachyonFile. 
 */
public enum ReadType {
  // Read the file and but do not cache it explicitly.
  NO_CACHE(1),
  // Read the file and cache it.
  CACHE(2);

  private final int mValue;

  private ReadType(int value) {
    mValue = value;
  }

  public int getValue() {
    return mValue;
  }

  public boolean isCache() {
    return mValue == CACHE.mValue;
  }

  public static ReadType getOpType(String op) throws IOException {
    if (op.equals("NO_CACHE")) {
      return NO_CACHE;
    } else if (op.equals("CACHE")) {
      return CACHE;
    }

    throw new IOException("Unknown ReadType : " + op);
  }
}
