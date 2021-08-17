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
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.exception.BlockInfoException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Command;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.StorageList;
import alluxio.grpc.WorkerLostStorageInfo;
import alluxio.master.Master;
import alluxio.metrics.Metric;
import alluxio.proto.meta.Block;
import alluxio.wire.Address;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Interface of the block master that manages the metadata for all the blocks and block workers in
 * Alluxio.
 */
public interface BlockMaster extends Master, ContainerIdGenerable {
  /**
   * @return the number of live workers
   */
  int getWorkerCount();

  /**
   * @return the number of lost workers
   */
  int getLostWorkerCount();

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
   * @return a list of {@link WorkerInfo} objects representing the live workers in Alluxio
   */
  List<WorkerInfo> getWorkerInfoList() throws UnavailableException;

  /**
   * @return a list of {@link WorkerInfo}s of lost workers
   */
  List<WorkerInfo> getLostWorkersInfoList() throws UnavailableException;

  /**
   * @return a set of live worker addresses
   */
  Set<WorkerNetAddress> getWorkerAddresses() throws UnavailableException;

  /**
   * Gets the worker information list for report CLI.
   *
   * @param options the GetWorkerReportOptions defines the info range
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
  List<WorkerInfo> getWorkerReport(GetWorkerReportOptions options)
      throws UnavailableException, InvalidArgumentException;

  /**
   * @return a list of worker lost storage information
   */
  List<WorkerLostStorageInfo> getWorkerLostStorage();

  /**
   * Removes blocks from workers.
   *
   * @param blockIds a list of block ids to remove from Alluxio space
   * @param delete whether to delete blocks' metadata in Master
   */
  void removeBlocks(List<Long> blockIds, boolean delete) throws UnavailableException;

  /**
   * Validates the integrity of blocks with respect to the validator. A warning will be printed if
   * blocks are invalid.
   *
   * @param validator a function returns true if the given block id is valid
   * @param repair if true, deletes the invalid blocks
   * @throws UnavailableException if the invalid blocks cannot be deleted
   */
  void validateBlocks(Function<Long, Boolean> validator, boolean repair)
      throws UnavailableException;

  /**
   * Marks a block as committed on a specific worker.
   *
   * @param workerId the worker id committing the block
   * @param usedBytesOnTier the updated used bytes on the tier of the worker
   * @param tierAlias the alias of the storage tier where the worker is committing the block to
   * @param mediumType the medium type where the worker is committing the block to
   * @param blockId the committing block id
   * @param length the length of the block
   * @throws NotFoundException if the workerId is not active
   */
  // TODO(binfan): check the logic is correct or not when commitBlock is a retry
  void commitBlock(long workerId, long usedBytesOnTier, String tierAlias,
      String mediumType, long blockId, long length)
      throws NotFoundException, UnavailableException;

  /**
   * Marks a block as committed, but without a worker location. This means the block is only in ufs.
   *
   * @param blockId the id of the block to commit
   * @param length the length of the block
   */
  void commitBlockInUFS(long blockId, long length) throws UnavailableException;

  /**
   * @param blockId the block id to get information for
   * @return the {@link BlockInfo} for the given block id
   * @throws BlockInfoException if the block info is not found
   */
  BlockInfo getBlockInfo(long blockId) throws BlockInfoException, UnavailableException;

  /**
   * Retrieves information for the given list of block ids.
   *
   * @param blockIds A list of block ids to retrieve the information for
   * @return A list of {@link BlockInfo} objects corresponding to the input list of block ids. The
   *         list is in the same order as the input list
   */
  List<BlockInfo> getBlockInfoList(List<Long> blockIds) throws UnavailableException;

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
   * @param currentBlocksOnLocation a mapping from storage tier alias to a list of blocks
   * @param lostStorage a mapping from storage tier alias to a list of lost storage paths
   * @param options the options that may contain worker configuration
   * @throws NotFoundException if workerId cannot be found
   */
  void workerRegister(long workerId, List<String> storageTiers,
      Map<String, Long> totalBytesOnTiers, Map<String, Long> usedBytesOnTiers,
      Map<Block.BlockLocation, List<Long>> currentBlocksOnLocation,
      Map<String, StorageList> lostStorage, RegisterWorkerPOptions options)
      throws NotFoundException;

  /**
   * Updates metadata when a worker periodically heartbeats with the master.
   *
   * @param workerId the worker id
   * @param capacityBytesOnTiers a mapping from tier alias to the capacity bytes
   * @param usedBytesOnTiers a mapping from tier alias to the used bytes
   * @param removedBlockIds a list of block ids removed from this worker
   * @param addedBlocks a mapping from tier alias to the added blocks
   * @param lostStorage a mapping from tier alias to lost storage paths
   * @param metrics worker metrics
   * @return an optional command for the worker to execute
   */
  Command workerHeartbeat(long workerId, Map<String, Long> capacityBytesOnTiers,
      Map<String, Long> usedBytesOnTiers, List<Long> removedBlockIds,
      Map<Block.BlockLocation, List<Long>> addedBlocks,
      Map<String, StorageList> lostStorage,
      List<Metric> metrics);

  /**
   * @param blockId the block ID
   * @return whether the block is considered lost in Alluxio
   */
  boolean isBlockLost(long blockId);

  /**
   * Returns an {@link Iterator} over the lost blocks.
   * Note that the iterator should not be shared across threads.
   *
   * @return an Iterator
   */
  Iterator<Long> getLostBlocksIterator();

  /**
   * @return the number of lost blocks in Alluxio
   */
  @VisibleForTesting
  int getLostBlocksCount();

  /**
   * Reports the ids of the blocks lost on workers.
   *
   * @param blockIds the ids of the lost blocks
   */
  void reportLostBlocks(List<Long> blockIds);

  /**
   * Registers callback functions to use when lost workers become alive.
   *
   * @param function the function to register
   */
  void registerLostWorkerFoundListener(Consumer<Address> function);

  /**
   * Registers callback functions to use when detecting lost workers.
   *
   * @param function the function to register
   */
  void registerWorkerLostListener(Consumer<Address> function);

  /**
   * Registers callback functions to use when workers register with configuration.
   *
   * @param function the function to register
   */
  void registerNewWorkerConfListener(BiConsumer<Address, List<ConfigProperty>> function);
}
