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

import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.master.file.FileSystemMasterOptions;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link CheckConsistencyPOptions}.
 */
public class CheckConsistencyContext extends OperationContext<CheckConsistencyPOptions.Builder> {
  // Prevent instantiation
  private CheckConsistencyContext() {
    super(null);
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private CheckConsistencyContext(CheckConsistencyPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link CheckConsistencyPOptions} with the corresponding master
   * options.
   *
   * @param optionsBuilder Builder for proto {@link CheckConsistencyPOptions} to merge with defaults
   * @return the instance of {@link CheckConsistencyContext} with default values for master
   */
  public static CheckConsistencyContext defaults(CheckConsistencyPOptions.Builder optionsBuilder) {
    CheckConsistencyPOptions masterOptions = FileSystemMasterOptions.checkConsistencyDefaults();
    CheckConsistencyPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new CheckConsistencyContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link CheckConsistencyContext} with default values for master
   */
  public static CheckConsistencyContext defaults() {
    CheckConsistencyPOptions masterOptions = FileSystemMasterOptions.checkConsistencyDefaults();
    return new CheckConsistencyContext(masterOptions.toBuilder());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
         .toString();
  }
}
