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
import alluxio.grpc.DecommissionWorkerPOptions;
import alluxio.util.FileSystemOptions;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link DecommissionWorkerPOptions}.
 */
public class DecommissionWorkerContext extends OperationContext<DecommissionWorkerPOptions.Builder,
        DecommissionWorkerContext> {

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private DecommissionWorkerContext(DecommissionWorkerPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * @param optionsBuilder Builder for proto {@link DecommissionWorkerPOptions}
   * @return the instance of {@link DecommissionWorkerContext} with the given options
   */
  public static DecommissionWorkerContext create(DecommissionWorkerPOptions.Builder optionsBuilder) {
    return new DecommissionWorkerContext(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link DecommissionWorkerPOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link DecommissionWorkerPOptions} to merge with defaults
   * @return the instance of {@link DecommissionWorkerContext} with default values for master
   */
  public static DecommissionWorkerContext mergeFrom(DecommissionWorkerPOptions.Builder optionsBuilder) {
    DecommissionWorkerPOptions masterOptions = FileSystemOptions.decommissionWorkerDefaults(Configuration.global());
    DecommissionWorkerPOptions.Builder mergedOptionsBuilder =
            masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return create(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link DecommissionWorkerContext} with default values for master
   */
  public static DecommissionWorkerContext defaults() {
    return create(FileSystemOptions.decommissionWorkerDefaults(Configuration.global()).toBuilder());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("ProtoOptions", getOptions().build())
            .toString();
  }
}
