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

package alluxio.client.block.policy;

import alluxio.annotation.PublicApi;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;

import javax.annotation.Nullable;

/**
 * <p>
 * Interface for determining the Alluxio worker location to serve a block write or UFS block read.
 * </p>
 *
 * <p>
 * {@link alluxio.client.file.FileInStream} uses this to determine where to read a UFS block.
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
     * @param conf Alluxio configuration
     * @return a new instance of {@link BlockLocationPolicy}
     */
    public static BlockLocationPolicy create(String blockLocationPolicyClass,
        AlluxioConfiguration conf) {
      try {
        Class<BlockLocationPolicy> clazz = (Class<BlockLocationPolicy>) Class
            .forName(blockLocationPolicyClass);
        return CommonUtils.createNewClassInstance(clazz, new Class[] {AlluxioConfiguration.class},
            new Object[] {conf});
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Gets the worker's network address for serving operations requested for the block.
   *
   * @param options the options to get a block worker network address for a block
   * @return the address of the worker to write to, null if no worker can be selected
   */
  @Nullable
  WorkerNetAddress getWorker(GetWorkerOptions options);
}
