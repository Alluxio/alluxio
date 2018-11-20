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

import alluxio.grpc.ListStatusPOptions;
import alluxio.master.file.FileSystemMasterOptions;
import com.google.common.base.MoreObjects;

public class ListStatusContext extends OperationContext<ListStatusPOptions.Builder> {
  // Prevent instantiation
  private ListStatusContext() {
    super(null);
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private ListStatusContext(ListStatusPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link ListStatusPOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link ListStatusPOptions} to embed
   * @return the instance of {@link ListStatusContext} with default values for master
   */
  public static ListStatusContext defaults(ListStatusPOptions.Builder optionsBuilder) {
    ListStatusPOptions masterOptions = FileSystemMasterOptions.getListStatusOptions();
    ListStatusPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new ListStatusContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link ListStatusContext} with default values for master
   */
  public static ListStatusContext defaults() {
    ListStatusPOptions masterOptions = FileSystemMasterOptions.getListStatusOptions();
    return new ListStatusContext(masterOptions.toBuilder());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .toString();
  }
}
