package alluxio;

/**
 * Utility class to help distinguish between different types of Alluxio processes at runtime.
 */
public class AlluxioProcess {
  public static Type sType = Type.CLIENT;

  public enum Type {
    CLIENT,
    MASTER,
    WORKER
  }

  /**
   * @param type the {@link Type} to use
   */
  public static void setType(Type type) {
    sType = type;
  }

  /**
   * @return the {@link Type} of the current process
   */
  public static Type getType() {
    return sType;
  }

  private AlluxioProcess() {} // prevent instantiation
}
