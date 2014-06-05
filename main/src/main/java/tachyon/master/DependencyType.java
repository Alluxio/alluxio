package tachyon.master;

import java.io.IOException;

public enum DependencyType {
  Wide(1), Narrow(2);

  public static DependencyType getDependencyType(int value) throws IOException {
    if (value == 1) {
      return Wide;
    } else if (value == 2) {
      return Narrow;
    }

    throw new IOException("Unknown DependencyType value " + value);
  }

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