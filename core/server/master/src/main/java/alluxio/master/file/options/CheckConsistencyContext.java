package alluxio.master.file.options;

import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.master.file.FileSystemMasterOptions;

public class CheckConsistencyContext extends OperationContext<CheckConsistencyPOptions.Builder> {
  // Prevent instantiation
  private CheckConsistencyContext() {
    super(null);
  }

  /**
   * Creates context with given option data.
   *
   * @param options options
   */
  private CheckConsistencyContext(CheckConsistencyPOptions options) {
    super(options.toBuilder());
  }

  /**
   * Merges and embeds the given {@link CheckConsistencyPOptions} with the corresponding master
   * options.
   *
   * @param options Proto {@link CheckConsistencyPOptions} to embed
   * @return the instance of {@link CheckConsistencyContext} with default values for master
   */
  public static CheckConsistencyContext defaults(CheckConsistencyPOptions options) {
    CheckConsistencyPOptions masterOptions = FileSystemMasterOptions.getCheckConsistencyOptions();
    CheckConsistencyPOptions mergedOptions = masterOptions.toBuilder().mergeFrom(options).build();
    return new CheckConsistencyContext(mergedOptions);
  }

  /**
   * @return the instance of {@link CheckConsistencyContext} with default values for master
   */
  public static CheckConsistencyContext defaults() {
    CheckConsistencyPOptions masterOptions = FileSystemMasterOptions.getCheckConsistencyOptions();
    return new CheckConsistencyContext(masterOptions);
  }
}
