package tachyon;

public enum DependencyType {
  Wide(1),
  Narrow(2);

  private final int mValue;

  private DependencyType(int value) {
    mValue = value;
  }

  /**
   * Get the integer value of this enum value.
   */
  public int getValue() {
    return mValue;
  }
}