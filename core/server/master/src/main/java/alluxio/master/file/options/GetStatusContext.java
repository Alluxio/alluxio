package alluxio.master.file.options;

import alluxio.grpc.GetStatusPOptions;
import alluxio.master.file.FileSystemMasterOptions;

public class GetStatusContext extends OperationContext<GetStatusPOptions.Builder> {
  // Prevent instantiation
  private GetStatusContext() {
    super(null);
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private GetStatusContext(GetStatusPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link GetStatusPOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link GetStatusPOptions} to embed
   * @return the instance of {@link GetStatusContext} with default values for master
   */
  public static GetStatusContext defaults(GetStatusPOptions.Builder optionsBuilder) {
    GetStatusPOptions masterOptions = FileSystemMasterOptions.getGetStatusOptions();
    GetStatusPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new GetStatusContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link GetStatusContext} with default values for master
   */
  public static GetStatusContext defaults() {
    GetStatusPOptions masterOptions = FileSystemMasterOptions.getGetStatusOptions();
    return new GetStatusContext(masterOptions.toBuilder());
  }
}
