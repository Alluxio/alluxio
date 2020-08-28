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

import alluxio.Constants;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.SetAttributePOptions;
import alluxio.util.FileSystemOptions;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link SetAttributePOptions}.
 */
public class SetAttributeContext
    extends OperationContext<SetAttributePOptions.Builder, SetAttributeContext> {

  private long mOperationTimeMs;
  private String mUfsFingerprint;

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private SetAttributeContext(SetAttributePOptions.Builder optionsBuilder) {
    super(optionsBuilder);
    mOperationTimeMs = System.currentTimeMillis();
    mUfsFingerprint = Constants.INVALID_UFS_FINGERPRINT;
  }

  /**
   * @param optionsBuilder Builder for proto {@link SetAttributePOptions}
   * @return the instance of {@link SetAttributeContext} with the given options
   */
  public static SetAttributeContext create(SetAttributePOptions.Builder optionsBuilder) {
    return new SetAttributeContext(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link SetAttributePOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link SetAttributePOptions} to merge with defaults
   * @return the instance of {@link SetAttributeContext} with default values for master
   */
  public static SetAttributeContext mergeFrom(SetAttributePOptions.Builder optionsBuilder) {
    SetAttributePOptions masterOptions =
        FileSystemOptions.setAttributeDefaults(ServerConfiguration.global());
    SetAttributePOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return create(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link SetAttributeContext} with default values for master
   */
  public static SetAttributeContext defaults() {
    return create(FileSystemOptions.setAttributeDefaults(ServerConfiguration.global()).toBuilder());
  }

  /**
   * @return the operation system time in ms
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }

  /**
   * Sets operation time.
   *
   * @param operationTimeMs operation system time in ms
   * @return the updated context instance
   */
  public SetAttributeContext setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
    return this;
  }

  /**
   * @return the ufs fingerprint
   */
  public String getUfsFingerprint() {
    return mUfsFingerprint;
  }

  /**
   * Sets ufs fingerprint.
   *
   * @param ufsFingerprint the ufs fingerprint
   * @return the updated context instance
   */
  public SetAttributeContext setUfsFingerprint(String ufsFingerprint) {
    mUfsFingerprint = ufsFingerprint;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .add("OperationTimeMs", mOperationTimeMs)
        .add("UfsFingerprint", mUfsFingerprint)
        .toString();
  }
}
