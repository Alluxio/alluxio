package alluxio.master.file.options;

import alluxio.grpc.SetAclPOptions;
import alluxio.master.file.FileSystemMasterOptions;

public class SetAclContext extends OperationContext<SetAclPOptions.Builder> {
  // Prevent instantiation
  private SetAclContext() {
    super(null);
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private SetAclContext(SetAclPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link SetAclPOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link SetAclPOptions} to embed
   * @return the instance of {@link SetAclContext} with default values for master
   */
  public static SetAclContext defaults(SetAclPOptions.Builder optionsBuilder) {
    SetAclPOptions masterOptions = FileSystemMasterOptions.getSetAclOptions();
    SetAclPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new SetAclContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link SetAclContext} with default values for master
   */
  public static SetAclContext defaults() {
    SetAclPOptions masterOptions = FileSystemMasterOptions.getSetAclOptions();
    return new SetAclContext(masterOptions.toBuilder());
  }
}
