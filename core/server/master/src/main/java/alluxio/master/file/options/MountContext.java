package alluxio.master.file.options;

import alluxio.grpc.MountPOptions;
import alluxio.master.file.FileSystemMasterOptions;

public class MountContext extends OperationContext<MountPOptions.Builder> {
  // Prevent instantiation
  private MountContext() {
    super(null);
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private MountContext(MountPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link MountPOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link MountPOptions} to embed
   * @return the instance of {@link MountContext} with default values for master
   */
  public static MountContext defaults(MountPOptions.Builder optionsBuilder) {
    MountPOptions masterOptions = FileSystemMasterOptions.getMountOptions();
    MountPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new MountContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link MountContext} with default values for master
   */
  public static MountContext defaults() {
    MountPOptions masterOptions = FileSystemMasterOptions.getMountOptions();
    return new MountContext(masterOptions.toBuilder());
  }
}
