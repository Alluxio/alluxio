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

  public static void setType(Type type) {
    sType = type;
  }

  public static Type getType() {
    return sType;
  }

  private AlluxioProcess() {} // prevent instantiation
}
