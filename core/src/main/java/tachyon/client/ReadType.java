package tachyon.client;

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
