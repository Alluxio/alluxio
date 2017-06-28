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

package alluxio.client.file.policy;

import alluxio.annotation.PublicApi;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileOutStream;
import alluxio.wire.WorkerNetAddress;

/**
 * <p>
 * Interface for the location policy of which workers a file's blocks are written into. A location
 * policy instance is used only once per file write.
 * </p>
 *
 * <p>
 * The {@link FileOutStream} calls {@link #getWorkerForNextBlock} to decide which worker to write
 * the next block per block write.
 * </p>
 *
 * <p>
 * A policy must have an empty constructor to be used as default policy.
 * </p>
 */
@PublicApi
// TODO(peis): Deprecate this and use BlockLocationPolicy in 2.0.
public interface FileWriteLocationPolicy {
  /**
   * Gets the worker's host name for the next block to write to.
   *
   * @param workerInfoList the info of the active workers
   * @param blockSizeBytes the size of the block in bytes
   * @return the address of the worker to write to, null if no worker can be selected
   */
  WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes);
}
