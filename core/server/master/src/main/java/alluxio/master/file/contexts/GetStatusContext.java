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
import alluxio.grpc.GetStatusPOptions;
import alluxio.util.FileSystemOptions;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link GetStatusPOptions}.
 */
public class GetStatusContext
    extends OperationContext<GetStatusPOptions.Builder, GetStatusContext> {
  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private GetStatusContext(GetStatusPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * @param optionsBuilder Builder for proto {@link GetStatusPOptions}
   * @return the instance of {@link GetStatusContext} with the given options
   */
  public static GetStatusContext create(GetStatusPOptions.Builder optionsBuilder) {
    return new GetStatusContext(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link GetStatusPOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link GetStatusPOptions} to merge with defaults
   * @return the instance of {@link GetStatusContext} with default values for master
   */
  public static GetStatusContext mergeFrom(GetStatusPOptions.Builder optionsBuilder) {
    GetStatusPOptions masterOptions =
        FileSystemOptions.getStatusDefaults(ServerConfiguration.global());
    GetStatusPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return create(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link GetStatusContext} with default values for master
   */
  public static GetStatusContext defaults() {
    return create(FileSystemOptions.getStatusDefaults(ServerConfiguration.global()).toBuilder());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .toString();
  }
}
