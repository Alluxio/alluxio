package alluxio.master.file.options;

import alluxio.grpc.LoadMetadataPOptions;
import alluxio.master.file.FileSystemMasterOptions;
import alluxio.underfs.UfsStatus;
import com.google.common.base.MoreObjects;

public class LoadMetadataContext extends OperationContext<LoadMetadataPOptions.Builder> {

  private UfsStatus mUfsStatus;

  // Prevent instantiation
  private LoadMetadataContext() {
    super(null);
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private LoadMetadataContext(LoadMetadataPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link LoadMetadataPOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link LoadMetadataPOptions} to embed
   * @return the instance of {@link LoadMetadataContext} with default values for master
   */
  public static LoadMetadataContext defaults(LoadMetadataPOptions.Builder optionsBuilder) {
    LoadMetadataPOptions masterOptions = FileSystemMasterOptions.getLoadMetadataOptions();
    LoadMetadataPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new LoadMetadataContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link LoadMetadataContext} with default values for master
   */
  public static LoadMetadataContext defaults() {
    LoadMetadataPOptions masterOptions = FileSystemMasterOptions.getLoadMetadataOptions();
    return new LoadMetadataContext(masterOptions.toBuilder());
  }

  /**
   * @return the Ufs status
   */
  public UfsStatus getUfsStatus() {
    return mUfsStatus;
  }

  /**
   * Sets {@link UfsStatus} for the directory
   *
   * @param ufsStatus Ufs status to set
   * @return the updated context instance
   */
  public LoadMetadataContext setUfsStatus(UfsStatus ufsStatus) {
    mUfsStatus = ufsStatus;
    return this;
  }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("ProtoOptions", getOptions().build())
            .add("ufsStatus", mUfsStatus)
            .toString();
    }
}
