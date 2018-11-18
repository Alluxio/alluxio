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
   * @param options options
   */
  private SetAclContext(SetAclPOptions options) {
    super(options.toBuilder());
  }

  /**
   * Merges and embeds the given {@link SetAclPOptions} with the corresponding master options.
   *
   * @param options Proto {@link SetAclPOptions} to embed
   * @return the instance of {@link SetAclContext} with default values for master
   */
  public static SetAclContext defaults(SetAclPOptions options) {
    SetAclPOptions masterOptions = FileSystemMasterOptions.getSetAclOptions();
    SetAclPOptions mergedOptions = masterOptions.toBuilder().mergeFrom(options).build();
    return new SetAclContext(mergedOptions);
  }

  /**
   * @return the instance of {@link SetAclContext} with default values for master
   */
  public static SetAclContext defaults() {
    SetAclPOptions masterOptions = FileSystemMasterOptions.getSetAclOptions();
    return new SetAclContext(masterOptions);
  }
}
