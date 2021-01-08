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
import alluxio.grpc.CheckAccessPOptions;
import alluxio.util.FileSystemOptions;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link CheckAccessPOptions}.
 */
public class CheckAccessContext
    extends OperationContext<CheckAccessPOptions.Builder, CheckAccessContext> {

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private CheckAccessContext(CheckAccessPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * @param optionsBuilder Builder for proto {@link CheckAccessPOptions}
   * @return the instance of {@link CheckAccessContext} with given options
   */
  public static CheckAccessContext create(CheckAccessPOptions.Builder optionsBuilder) {
    return new CheckAccessContext(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link CheckAccessPOptions} with the corresponding master
   * options.
   *
   * @param optionsBuilder Builder for proto {@link CheckAccessPOptions} to merge with defaults
   * @return the instance of {@link CheckAccessContext} with default values for master
   */
  public static CheckAccessContext mergeFrom(CheckAccessPOptions.Builder optionsBuilder) {
    CheckAccessPOptions masterOptions =
        FileSystemOptions.checkAccessDefaults(ServerConfiguration.global());
    CheckAccessPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return create(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link CheckAccessContext} with default values for master
   */
  public static CheckAccessContext defaults() {
    return create(FileSystemOptions
        .checkAccessDefaults(ServerConfiguration.global()).toBuilder());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .toString();
  }
}
