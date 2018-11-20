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

import alluxio.grpc.DeletePOptions;
import alluxio.master.file.FileSystemMasterOptions;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link DeletePOptions}.
 */
public class DeleteContext extends OperationContext<DeletePOptions.Builder> {

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private DeleteContext(DeletePOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link DeletePOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link DeletePOptions} to merge with defaults
   * @return the instance of {@link DeleteContext} with default values for master
   */
  public static DeleteContext defaults(DeletePOptions.Builder optionsBuilder) {
    DeletePOptions masterOptions = FileSystemMasterOptions.deleteDefaults();
    DeletePOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new DeleteContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link DeleteContext} with default values for master
   */
  public static DeleteContext defaults() {
    DeletePOptions masterOptions = FileSystemMasterOptions.deleteDefaults();
    return new DeleteContext(masterOptions.toBuilder());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .toString();
  }
}
