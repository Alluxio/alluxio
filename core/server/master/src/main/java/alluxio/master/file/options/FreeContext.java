package alluxio.master.file.options;

import alluxio.grpc.FreePOptions;
import alluxio.master.file.FileSystemMasterOptions;

public class FreeContext extends OperationContext<FreePOptions.Builder> {
  // Prevent instantiation
  private FreeContext() {
    super(null);
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private FreeContext(FreePOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link FreePOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link FreePOptions} to embed
   * @return the instance of {@link FreeContext} with default values for master
   */
  public static FreeContext defaults(FreePOptions.Builder optionsBuilder) {
    FreePOptions masterOptions = FileSystemMasterOptions.getFreeOptions();
    FreePOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new FreeContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link FreeContext} with default values for master
   */
  public static FreeContext defaults() {
    FreePOptions masterOptions = FileSystemMasterOptions.getFreeOptions();
    return new FreeContext(masterOptions.toBuilder());
  }
}
