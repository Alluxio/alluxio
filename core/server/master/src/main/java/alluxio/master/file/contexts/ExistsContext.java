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

import alluxio.conf.Configuration;
import alluxio.grpc.ExistsPOptions;
import alluxio.util.FileSystemOptionsUtils;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link ExistsPOptions}.
 */
public class ExistsContext
    extends OperationContext<ExistsPOptions.Builder, ExistsContext> {

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private ExistsContext(ExistsPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * @param optionsBuilder Builder for proto {@link ExistsPOptions}
   * @return the instance of {@link ExistsContext} with given options
   */
  public static ExistsContext create(ExistsPOptions.Builder optionsBuilder) {
    return new ExistsContext(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link ExistsPOptions} with the corresponding master
   * options.
   *
   * @param optionsBuilder Builder for proto {@link ExistsPOptions} to merge with defaults
   * @return the instance of {@link ExistsContext} with default values for master
   */
  public static ExistsContext mergeFrom(ExistsPOptions.Builder optionsBuilder) {
    ExistsPOptions masterOptions =
        FileSystemOptionsUtils.existsDefaults(Configuration.global());
    ExistsPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return create(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link ExistsContext} with default values for master
   */
  public static ExistsContext defaults() {
    return create(FileSystemOptionsUtils
        .existsDefaults(Configuration.global()).toBuilder());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .toString();
  }
}
