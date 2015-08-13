package tachyon.client.next;

/**
 * Specifies the type of data interaction with Tachyon. Reads may use all three cache types. A
 * {@link CacheType#PROMOTE} cache type on write is the same as {@link CacheType#CACHE}.
 */
public enum CacheType {
  /** Write to Tachyon */
  CACHE(1),

  /** Do not write to Tachyon */
  NO_CACHE(2),

  /** Cache or Promote to highest tier if in Tachyon already */
  PROMOTE(3);

  private final int mValue;
  CacheType(int value) {
    mValue = value;
  }
}
