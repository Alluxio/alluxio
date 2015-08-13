package tachyon.client.next;

/**
 * Specifies the type of data interaction with Tachyon's Under Storage. This is not applicable
 * for reads.
 */
public enum UnderStorageType {
  /** Write to Under Storage synchronously */
  PERSIST(1),

  /** Do not write to Under Storage */
  NO_PERSIST(2);

  private final int mValue;
  UnderStorageType(int value) {
    mValue = value;
  }
}
