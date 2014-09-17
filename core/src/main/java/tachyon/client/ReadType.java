package tachyon.client;

import java.io.IOException;

/**
 * Different read types for a TachyonFile.
 */
public enum ReadType {
  /**
   * Read the file and but do not cache it explicitly.
   */
  NO_CACHE(1),
  /**
   * Read the file and cache it.
   */
  CACHE(2);

  /**
   * Parse the read type
   * 
   * @param op the String format of the read type
   * @return the read type
   * @throws IOException
   */
  public static ReadType getOpType(String op) throws IOException {
    if (op.equals("NO_CACHE")) {
      return NO_CACHE;
    } else if (op.equals("CACHE")) {
      return CACHE;
    }

    throw new IOException("Unknown ReadType : " + op);
  }

  private final int mValue;

  private ReadType(int value) {
    mValue = value;
  }

  /**
   * Return the value of the read type
   * 
   * @return the read type value
   */
  public int getValue() {
    return mValue;
  }

  /**
   * @return true if the read type is CACHE, false otherwise
   */
  public boolean isCache() {
    return mValue == CACHE.mValue;
  }
}
