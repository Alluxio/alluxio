package alluxio.master.file.options;

import alluxio.grpc.FileSystemHeartbeatPOptions;
import com.google.common.base.MoreObjects;

public class WorkerHeartbeatContext extends OperationContext<FileSystemHeartbeatPOptions.Builder> {
  // Prevent instantiation
  private WorkerHeartbeatContext() {
    super(FileSystemHeartbeatPOptions.newBuilder());
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private WorkerHeartbeatContext(FileSystemHeartbeatPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link FileSystemHeartbeatPOptions} with the corresponding master
   * options.
   *
   * @param optionsBuilder Builder for proto {@link FileSystemHeartbeatPOptions} to embed
   * @return the instance of {@link WorkerHeartbeatContext} with default values for master
   */
  public static WorkerHeartbeatContext defaults(
      FileSystemHeartbeatPOptions.Builder optionsBuilder) {
    return new WorkerHeartbeatContext(optionsBuilder);
  }

  /**
   * @return the instance of {@link WorkerHeartbeatContext} with default values for master
   */
  public static WorkerHeartbeatContext defaults() {
    return new WorkerHeartbeatContext();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("ProtoOptions", getOptions().build()).toString();
  }
}
