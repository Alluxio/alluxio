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
import alluxio.grpc.RenamePOptions;
import alluxio.util.FileSystemOptions;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link RenamePOptions}.
 */
public class RenameContext extends OperationContext<RenamePOptions.Builder, RenameContext> {

  private long mOperationTimeMs;
  private boolean mPersist;

  /**
   * Creates rename context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private RenameContext(RenamePOptions.Builder optionsBuilder) {
    super(optionsBuilder);
    mOperationTimeMs = System.currentTimeMillis();
    mPersist = optionsBuilder.getPersist();
  }

  /**
   * @param optionsBuilder Builder for proto {@link RenamePOptions}
   * @return the instance of {@link RenameContext} with the given options
   */
  public static RenameContext create(RenamePOptions.Builder optionsBuilder) {
    return new RenameContext(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link RenamePOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link RenamePOptions} to merge with defaults
   * @return the instance of {@link RenameContext} with default values for master
   */
  public static RenameContext mergeFrom(RenamePOptions.Builder optionsBuilder) {
    RenamePOptions masterOptions = FileSystemOptions.renameDefaults(ServerConfiguration.global());
    RenamePOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return create(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link RenameContext} with default values for master
   */
  public static RenameContext defaults() {
    return create(FileSystemOptions.renameDefaults(ServerConfiguration.global()).toBuilder());
  }

  /**
   * @return the operation system time in ms
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }

  /**
   * @return true if a persist operation should follow the rename
   */
  public boolean getPersist() {
    return mPersist;
  }

  /**
   * Sets operation time.
   *
   * @param operationTimeMs operation system time in ms
   * @return the updated context instance
   */
  public RenameContext setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .add("OperationTimeMs", mOperationTimeMs)
        .toString();
  }
}
