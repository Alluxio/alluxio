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
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;

/**
 * <p>
 * Interface for determining the Alluxio worker location to serve a block write or UFS block read.
 * </p>
 *
 * <p>
 * A policy must have an empty constructor to be used as default policy.
 * </p>
 */
@PublicApi
public interface BlockLocationPolicy {

  /**
   * The factory for the {@link BlockLocationPolicy}.
   */
  class Factory {
    private Factory() {} // prevent instantiation

    /**
     * Factory for creating {@link BlockLocationPolicy}.
     *
     * @param policyClassNameWithShard the block location policy class name. Optionally, it can also
     *        contains the number of shards that a block's traffic can be sharded. For example:
     *        alluxio.client.file.policy.DeterministicHashPolicy,
     *        alluxio.client.file.policy.DeterministicHashPolicy@5,
     *        alluxio.client.file.policy.LocalFirstPolicy.
     *        Note that DeterministicHashPolicy is the only policy that accepts a sharding factor
     *        that is greater than 1.
     * @return a new instance of {@link BlockLocationPolicy}
     */
    public static BlockLocationPolicy create(String policyClassNameWithShard) {
      String[] parts = policyClassNameWithShard.split("@");
      if (parts.length > 2) {
        throw new IllegalArgumentException(
            String.format("%s is a illegal block location policy name.", policyClassNameWithShard));
      }
      Integer numShards = 1;
      if (parts.length == 2) {
        numShards = Integer.valueOf(parts[1]);
      }
      try {
        Class<BlockLocationPolicy> clazz = (Class<BlockLocationPolicy>) Class.forName(parts[0]);
        if (numShards > 1) {
          return CommonUtils
              .createNewClassInstance(clazz, new Class[] {Integer.class}, new Object[] {numShards});
        } else {
          return CommonUtils.createNewClassInstance(clazz, new Class[] {}, new Object[] {});
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Gets the worker's host name for serving operations requested for the block.
   *
   * @param workerInfoList the info of the active workers
   * @param blockId the block ID
   * @param blockSizeBytes the size of the block in bytes
   * @return the address of the worker to write to, null if no worker can be selected
   */
  WorkerNetAddress getWorkerForBlock(Iterable<BlockWorkerInfo> workerInfoList, long blockId,
      long blockSizeBytes);
}
