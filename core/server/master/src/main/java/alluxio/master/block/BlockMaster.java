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

package alluxio.master.block;

import alluxio.StorageTierAssoc;
import alluxio.exception.BlockInfoException;
import alluxio.exception.NoWorkerException;
import alluxio.master.Master;
import alluxio.thrift.Command;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface of the block master that manages the metadata for all the blocks and block workers in
 * Alluxio.
 */
public interface BlockMaster extends Master, ContainerIdGenerable {
  /**
   * @return the number of workers
   */
  int getWorkerCount();

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
  List<WorkerInfo> getWorkerInfoList();

  /**
   * @return the total capacity (in bytes) on all tiers, on all workers of Alluxio
   */
  long getCapacityBytes();

  /**
   * @return the global storage tier mapping
   */
  StorageTierAssoc getGlobalStorageTierAssoc();

  /**
   * @return the total used bytes on all tiers, on all workers of Alluxio
   */
  long getUsedBytes();

  /**
   * @return a list of {@link WorkerInfo}s of lost workers
   */
  List<WorkerInfo> getLostWorkersInfoList();

  /**
   * Removes blocks from workers.
   *
   * @param blockIds a list of block ids to remove from Alluxio space
   * @param delete whether to delete blocks' metadata in Master
   */
  void removeBlocks(List<Long> blockIds, boolean delete);

  /**
   * Marks a block as committed on a specific worker.
   *
   * @param workerId the worker id committing the block
   * @param usedBytesOnTier the updated used bytes on the tier of the worker
   * @param tierAlias the alias of the storage tier where the worker is committing the block to
   * @param blockId the committing block id
   * @param length the length of the block
   * @throws NoWorkerException if the workerId is not active
   */
  // TODO(binfan): check the logic is correct or not when commitBlock is a retry
  void commitBlock(long workerId, long usedBytesOnTier, String tierAlias, long blockId, long
      length) throws NoWorkerException;

  /**
   * Marks a block as committed, but without a worker location. This means the block is only in ufs.
   *
   * @param blockId the id of the block to commit
   * @param length the length of the block
   */
  void commitBlockInUFS(long blockId, long length);

  /**
   * @param blockId the block id to get information for
   * @return the {@link BlockInfo} for the given block id
   * @throws BlockInfoException if the block info is not found
   */
  BlockInfo getBlockInfo(long blockId) throws BlockInfoException;

  /**
   * Retrieves information for the given list of block ids.
   *
   * @param blockIds A list of block ids to retrieve the information for
   * @return A list of {@link BlockInfo} objects corresponding to the input list of block ids. The
   *         list is in the same order as the input list
   */
  List<BlockInfo> getBlockInfoList(List<Long> blockIds);

  /**
   * @return the total bytes on each storage tier
   */
  Map<String, Long> getTotalBytesOnTiers();

  /**
   * @return the used bytes on each storage tier
   */
  Map<String, Long> getUsedBytesOnTiers();

  /**
   * Returns a worker id for the given worker, creating one if the worker is new.
   *
   * @param workerNetAddress the worker {@link WorkerNetAddress}
   * @return the worker id for this worker
   */
  long getWorkerId(WorkerNetAddress workerNetAddress);

  /**
   * Updates metadata when a worker registers with the master.
   *
   * @param workerId the worker id of the worker registering
   * @param storageTiers a list of storage tier aliases in order of their position in the worker's
   *        hierarchy
   * @param totalBytesOnTiers a mapping from storage tier alias to total bytes
   * @param usedBytesOnTiers a mapping from storage tier alias to the used byes
   * @param currentBlocksOnTiers a mapping from storage tier alias to a list of blocks
   * @throws NoWorkerException if workerId cannot be found
   */
  void workerRegister(long workerId, List<String> storageTiers,
      Map<String, Long> totalBytesOnTiers, Map<String, Long> usedBytesOnTiers,
      Map<String, List<Long>> currentBlocksOnTiers) throws NoWorkerException;

  /**
   * Updates metadata when a worker periodically heartbeats with the master.
   *
   * @param workerId the worker id
   * @param usedBytesOnTiers a mapping from tier alias to the used bytes
   * @param removedBlockIds a list of block ids removed from this worker
   * @param addedBlocksOnTiers a mapping from tier alias to the added blocks
   * @return an optional command for the worker to execute
   */
  Command workerHeartbeat(long workerId, Map<String, Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<String, List<Long>> addedBlocksOnTiers);

  /**
   * @return the block ids of lost blocks in Alluxio
   */
  Set<Long> getLostBlocks();

  /**
   * Reports the ids of the blocks lost on workers.
   *
   * @param blockIds the ids of the lost blocks
   */
  void reportLostBlocks(List<Long> blockIds);
}
