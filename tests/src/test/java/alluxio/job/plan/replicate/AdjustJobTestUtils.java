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

package alluxio.job.plan.replicate;

import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystemContext;
import alluxio.resource.CloseableResource;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

/**
 * Utils for replication integration tests.
 */
public final class AdjustJobTestUtils {

  private AdjustJobTestUtils() {} // prevent instantiation

  /**
   * Queries block master a given block's {@link BlockInfo}.
   *
   * @param blockId block ID
   * @param context the file system context
   * @return the {@link BlockInfo} of this block
   * @throws Exception if any error happens
   */
  public static BlockInfo getBlock(long blockId, FileSystemContext context) throws Exception {
    try (CloseableResource<BlockMasterClient> blockMasterClientResource = context
        .acquireBlockMasterClientResource()) {
      return blockMasterClientResource.get().getBlockInfo(blockId);
    }
  }

  /**
   * Checks whether a block is stored on a given worker.
   *
   * @param blockId block ID
   * @param address worker address
   * @param context handler of BlockStoreContext instance
   * @return true if block is on the given worker, false otherwise
   * @throws Exception if any error happens
   */
  public static boolean hasBlock(long blockId, WorkerNetAddress address, FileSystemContext context)
      throws Exception {
    try (CloseableResource<BlockMasterClient> master = context.acquireBlockMasterClientResource()) {
      BlockInfo blockInfo = master.get().getBlockInfo(blockId);
      return !blockInfo.getLocations().isEmpty();
    }
  }
}
