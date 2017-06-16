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

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A policy that returns the worker with the most available bytes.
 */
// TODO(peis): Move the BlockLocationPolicy implementation to alluxio.client.block.policy.
@ThreadSafe
public final class MostAvailableFirstPolicy
    implements FileWriteLocationPolicy, BlockLocationPolicy {

  /**
   * Constructs a new {@link MostAvailableFirstPolicy}.
   */
  public MostAvailableFirstPolicy() {}

  @Override
  public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes) throws UnavailableException {
    long mostAvailableBytes = -1;
    WorkerNetAddress result = null;
    for (BlockWorkerInfo workerInfo : workerInfoList) {
      if (workerInfo.getCapacityBytes() - workerInfo.getUsedBytes() > mostAvailableBytes) {
        mostAvailableBytes = workerInfo.getCapacityBytes() - workerInfo.getUsedBytes();
        result = workerInfo.getNetAddress();
      }
    }
    if (result == null) {
      throw new UnavailableException(
          ExceptionMessage.NO_SPACE_FOR_BLOCK_ON_WORKER.getMessage(blockSizeBytes));
    }
    return result;
  }

  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) throws UnavailableException {
    return getWorkerForNextBlock(options.getBlockWorkerInfos(), options.getBlockSize());
  }

  @Override
  public boolean equals(Object o) {
    return this == o || o instanceof MostAvailableFirstPolicy;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).toString();
  }
}
