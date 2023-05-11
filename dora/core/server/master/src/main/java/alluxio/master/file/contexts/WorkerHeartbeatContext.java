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

import alluxio.grpc.FileSystemHeartbeatPOptions;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link FileSystemHeartbeatPOptions}.
 */
public class WorkerHeartbeatContext
    extends OperationContext<FileSystemHeartbeatPOptions.Builder, WorkerHeartbeatContext> {
  // Prevent instantiation
  private WorkerHeartbeatContext() {
    super(FileSystemHeartbeatPOptions.newBuilder());
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private WorkerHeartbeatContext(FileSystemHeartbeatPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link FileSystemHeartbeatPOptions} with the corresponding master
   * options.
   *
   * @param optionsBuilder Builder for proto {@link FileSystemHeartbeatPOptions} to merge with
   *        defaults
   * @return the instance of {@link WorkerHeartbeatContext} with default values for master
   */
  public static WorkerHeartbeatContext create(
      FileSystemHeartbeatPOptions.Builder optionsBuilder) {
    return new WorkerHeartbeatContext(optionsBuilder);
  }

  /**
   * @return the instance of {@link WorkerHeartbeatContext} with default values for master
   */
  public static WorkerHeartbeatContext defaults() {
    return new WorkerHeartbeatContext();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("ProtoOptions", getOptions().build()).toString();
  }
}
