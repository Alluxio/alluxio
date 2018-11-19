package alluxio.master.file.options;

import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.master.file.FileSystemMasterOptions;
import com.google.common.base.MoreObjects;

public class CheckConsistencyContext extends OperationContext<CheckConsistencyPOptions.Builder> {
  // Prevent instantiation
  private CheckConsistencyContext() {
    super(null);
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private CheckConsistencyContext(CheckConsistencyPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link CheckConsistencyPOptions} with the corresponding master
   * options.
   *
   * @param optionsBuilder Builder for proto {@link CheckConsistencyPOptions} to embed
   * @return the instance of {@link CheckConsistencyContext} with default values for master
   */
  public static CheckConsistencyContext defaults(CheckConsistencyPOptions.Builder optionsBuilder) {
    CheckConsistencyPOptions masterOptions = FileSystemMasterOptions.getCheckConsistencyOptions();
    CheckConsistencyPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new CheckConsistencyContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link CheckConsistencyContext} with default values for master
   */
  public static CheckConsistencyContext defaults() {
    CheckConsistencyPOptions masterOptions = FileSystemMasterOptions.getCheckConsistencyOptions();
    return new CheckConsistencyContext(masterOptions.toBuilder());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
         .toString();
  }
}
