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

import alluxio.grpc.FreePOptions;
import alluxio.master.file.FileSystemMasterOptions;
import com.google.common.base.MoreObjects;

public class FreeContext extends OperationContext<FreePOptions.Builder> {
  // Prevent instantiation
  private FreeContext() {
    super(null);
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private FreeContext(FreePOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link FreePOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link FreePOptions} to embed
   * @return the instance of {@link FreeContext} with default values for master
   */
  public static FreeContext defaults(FreePOptions.Builder optionsBuilder) {
    FreePOptions masterOptions = FileSystemMasterOptions.getFreeOptions();
    FreePOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new FreeContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link FreeContext} with default values for master
   */
  public static FreeContext defaults() {
    FreePOptions masterOptions = FileSystemMasterOptions.getFreeOptions();
    return new FreeContext(masterOptions.toBuilder());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .toString();
  }
}
