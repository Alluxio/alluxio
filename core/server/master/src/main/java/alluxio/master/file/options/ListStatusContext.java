package alluxio.master.file.options;

import alluxio.grpc.ListStatusPOptions;
import alluxio.master.file.FileSystemMasterOptions;

public class ListStatusContext extends OperationContext<ListStatusPOptions.Builder> {
  // Prevent instantiation
  private ListStatusContext() {
    super(null);
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private ListStatusContext(ListStatusPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link ListStatusPOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link ListStatusPOptions} to embed
   * @return the instance of {@link ListStatusContext} with default values for master
   */
  public static ListStatusContext defaults(ListStatusPOptions.Builder optionsBuilder) {
    ListStatusPOptions masterOptions = FileSystemMasterOptions.getListStatusOptions();
    ListStatusPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new ListStatusContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link ListStatusContext} with default values for master
   */
  public static ListStatusContext defaults() {
    ListStatusPOptions masterOptions = FileSystemMasterOptions.getListStatusOptions();
    return new ListStatusContext(masterOptions.toBuilder());
  }
}
