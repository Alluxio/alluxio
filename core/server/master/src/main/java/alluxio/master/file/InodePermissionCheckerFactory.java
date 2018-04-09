package alluxio.master.file;

/**
 * Factory for {@link InodePermissionChecker}.
 */
public final class InodePermissionCheckerFactory {
  /**
   * @return a new instance of {@link InodePermissionChecker}
   */
  public static InodePermissionChecker create() {
    return new DefaultInodePermissionChecker();
  }

  private InodePermissionCheckerFactory() {
    // Prevent initialization.
  }
}
