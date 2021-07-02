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
import alluxio.grpc.FreePOptions;
import alluxio.util.FileSystemOptions;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link FreePOptions}.
 */
public class FreeContext extends OperationContext<FreePOptions.Builder, FreeContext> {

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private FreeContext(FreePOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * @param optionsBuilder Builder for proto {@link FreePOptions}
   * @return the instance of {@link FreeContext} with the given options
   */
  public static FreeContext create(FreePOptions.Builder optionsBuilder) {
    return new FreeContext(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link FreePOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link FreePOptions} to merge with defaults
   * @return the instance of {@link FreeContext} with default values for master
   */
  public static FreeContext mergeFrom(FreePOptions.Builder optionsBuilder) {
    FreePOptions masterOptions = FileSystemOptions.freeDefaults(ServerConfiguration.global());
    FreePOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return create(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link FreeContext} with default values for master
   */
  public static FreeContext defaults() {
    return create(FileSystemOptions.freeDefaults(ServerConfiguration.global()).toBuilder());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .toString();
  }
}
