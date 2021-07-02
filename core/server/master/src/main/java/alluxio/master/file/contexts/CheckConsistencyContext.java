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
import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.util.FileSystemOptions;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link CheckConsistencyPOptions}.
 */
public class CheckConsistencyContext
    extends OperationContext<CheckConsistencyPOptions.Builder, CheckConsistencyContext> {

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private CheckConsistencyContext(CheckConsistencyPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * @param optionsBuilder Builder for proto {@link CheckConsistencyPOptions}
   * @return the instance of {@link CheckConsistencyContext} with given options
   */
  public static CheckConsistencyContext create(CheckConsistencyPOptions.Builder optionsBuilder) {
    return new CheckConsistencyContext(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link CheckConsistencyPOptions} with the corresponding master
   * options.
   *
   * @param optionsBuilder Builder for proto {@link CheckConsistencyPOptions} to merge with defaults
   * @return the instance of {@link CheckConsistencyContext} with default values for master
   */
  public static CheckConsistencyContext mergeFrom(CheckConsistencyPOptions.Builder optionsBuilder) {
    CheckConsistencyPOptions masterOptions =
        FileSystemOptions.checkConsistencyDefaults(ServerConfiguration.global());
    CheckConsistencyPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return create(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link CheckConsistencyContext} with default values for master
   */
  public static CheckConsistencyContext defaults() {
    return create(FileSystemOptions
        .checkConsistencyDefaults(ServerConfiguration.global()).toBuilder());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
         .toString();
  }
}
