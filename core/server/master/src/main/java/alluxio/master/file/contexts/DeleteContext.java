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
import alluxio.grpc.DeletePOptions;
import alluxio.util.FileSystemOptions;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link DeletePOptions}.
 */
public class DeleteContext extends OperationContext<DeletePOptions.Builder, DeleteContext> {

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private DeleteContext(DeletePOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * @param optionsBuilder Builder for proto {@link DeletePOptions}
   * @return the instance of {@link DeleteContext} with given options
   */
  public static DeleteContext create(DeletePOptions.Builder optionsBuilder) {
    return new DeleteContext(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link DeletePOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link DeletePOptions} to merge with defaults
   * @return the instance of {@link DeleteContext} with default values for master
   */
  public static DeleteContext mergeFrom(DeletePOptions.Builder optionsBuilder) {
    DeletePOptions masterOptions = FileSystemOptions.deleteDefaults(ServerConfiguration.global());
    DeletePOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return create(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link DeleteContext} with default values for master
   */
  public static DeleteContext defaults() {
    return create(FileSystemOptions.deleteDefaults(ServerConfiguration.global()).toBuilder());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .toString();
  }
}
