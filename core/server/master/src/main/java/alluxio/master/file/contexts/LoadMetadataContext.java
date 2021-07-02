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

package alluxio.master.file.contexts;

import alluxio.conf.ServerConfiguration;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.underfs.UfsStatus;
import alluxio.util.FileSystemOptions;

import com.google.common.base.MoreObjects;

import javax.annotation.Nullable;

/**
 * Used to merge and wrap {@link LoadMetadataPOptions}.
 */
public class LoadMetadataContext
    extends OperationContext<LoadMetadataPOptions.Builder, LoadMetadataContext> {

  private UfsStatus mUfsStatus;

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private LoadMetadataContext(LoadMetadataPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * @param optionsBuilder Builder for proto {@link LoadMetadataPOptions}
   * @return the instance of {@link LoadMetadataContext} with the given options
   */
  public static LoadMetadataContext create(LoadMetadataPOptions.Builder optionsBuilder) {
    return new LoadMetadataContext(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link LoadMetadataPOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link LoadMetadataPOptions} to embed
   * @return the instance of {@link LoadMetadataContext} with default values for master
   */
  public static LoadMetadataContext mergeFrom(LoadMetadataPOptions.Builder optionsBuilder) {
    LoadMetadataPOptions masterOptions =
        FileSystemOptions.loadMetadataDefaults(ServerConfiguration.global());
    LoadMetadataPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return create(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link LoadMetadataContext} with default values for master
   */
  public static LoadMetadataContext defaults() {
    return create(FileSystemOptions
        .loadMetadataDefaults(ServerConfiguration.global()).toBuilder());
  }

  /**
   * @return the Ufs status
   */
  @Nullable
  public UfsStatus getUfsStatus() {
    return mUfsStatus;
  }

  /**
   * Sets {@link UfsStatus} for the directory.
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
