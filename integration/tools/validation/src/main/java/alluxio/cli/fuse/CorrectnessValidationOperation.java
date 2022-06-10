package alluxio.cli.fuse;

/**
 * The valid operations that can be tested by the Fuse correctness validation tool.
 */
public enum CorrectnessValidationOperation {
  READ("Read"),
  WRITE("Write"),
  ;

  private final String mName;

  CorrectnessValidationOperation(String name) {
    mName = name;
  }

  @Override
  public String toString() {
    return mName;
  }

  /**
   * Convert operation string to {@link CorrectnessValidationOperation}
   * @param operationStr the operation in string format
   * @return the operation
   */
  public static CorrectnessValidationOperation fromString(String operationStr) {
    System.out.println(operationStr);
    for (CorrectnessValidationOperation type : CorrectnessValidationOperation.values()) {
      System.out.println(type.toString());
      if (type.toString().equalsIgnoreCase(operationStr)) {
        return type;
      }
    }
    throw new IllegalArgumentException(String.format("Operation %s is not valid.", operationStr));
  }
}
