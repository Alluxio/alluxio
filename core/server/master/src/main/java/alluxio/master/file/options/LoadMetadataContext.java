/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

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
