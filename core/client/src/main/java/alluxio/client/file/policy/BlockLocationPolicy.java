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
import alluxio.wire.WorkerNetAddress;

/**
 * <p>
 * Interface for determine the Alluxio worker location to serve a block write or UFS block read.
 * </p>
 *
 * <p>
 * A policy must have an empty constructor to be used as default policy.
 * </p>
 */
@PublicApi
public interface BlockLocationPolicy {
  /**
   * Gets the worker's host name for serve operations requested for the block.
   *
   * @param workerInfoList the info of the active workers
   * @param blockId the block ID
   * @param blockSizeBytes the size of the block in bytes
   * @return the address of the worker to write to, null if no worker can be selected
   */
  WorkerNetAddress getWorkerForBlock(Iterable<BlockWorkerInfo> workerInfoList, long blockId,
      long blockSizeBytes);
}
