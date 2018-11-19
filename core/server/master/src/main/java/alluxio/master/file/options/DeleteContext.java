package alluxio.master.file.options;

import alluxio.grpc.DeletePOptions;
import alluxio.master.file.FileSystemMasterOptions;
import com.google.common.base.MoreObjects;

public class DeleteContext extends OperationContext<DeletePOptions.Builder> {
  // Prevent instantiation
  private DeleteContext() {
    super(null);
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private DeleteContext(DeletePOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link DeletePOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link DeletePOptions} to embed
   * @return the instance of {@link DeleteContext} with default values for master
   */
  public static DeleteContext defaults(DeletePOptions.Builder optionsBuilder) {
    DeletePOptions masterOptions = FileSystemMasterOptions.getDeleteOptions();
    DeletePOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new DeleteContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link DeleteContext} with default values for master
   */
  public static DeleteContext defaults() {
    DeletePOptions masterOptions = FileSystemMasterOptions.getDeleteOptions();
    return new DeleteContext(masterOptions.toBuilder());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .toString();
  }
}
