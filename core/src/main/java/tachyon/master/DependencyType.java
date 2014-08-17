package tachyon.master;

import java.io.IOException;

/**
 * The type of Dependency, Wide and Narrow. The difference is that how many input files does each
 * output file require in the Dependency. In Narrow Dependency, each output file only requires one
 * input file, while in Wide Dependency it requires more than one.
 */
public enum DependencyType {
  /**
   * Wide Dependency means that each output file requires more than one input files.
   */
  Wide(1),
  /**
   * Narrow Dependency means that each output file only requires one input file.
   */
  Narrow(2);

  /**
   * Get the enum value with the given integer value. It will check the legality.
   * 
   * @param value
   *          The integer value, 1 or 2, means Wide or Narrow
   * @return the enum value of DependencyType
   * @throws IOException
   */
  public static DependencyType getDependencyType(int value) throws IOException {
    if (value == 1) {
      return Wide;
    } else if (value == 2) {
      return Narrow;
    }

    throw new IOException("Unknown DependencyType value " + value);
  }

  private final int VALUE;

  private DependencyType(int value) {
    VALUE = value;
  }

  /**
   * Get the integer value of this enum value.
   */
  public int getValue() {
    return VALUE;
  }
}