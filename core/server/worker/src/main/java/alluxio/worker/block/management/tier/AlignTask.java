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

package alluxio.worker.block.management.tier;

import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.annotator.BlockOrder;
import alluxio.worker.block.evictor.BlockTransferInfo;
import alluxio.worker.block.management.AbstractBlockManagementTask;
import alluxio.worker.block.management.BlockManagementTaskResult;
import alluxio.worker.block.management.BlockOperationResult;
import alluxio.worker.block.management.BlockOperationType;
import alluxio.worker.block.management.ManagementTaskCoordinator;
import alluxio.worker.block.management.StoreLoadTracker;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A BlockStore management task that swaps blocks between tiers in order to align
 * tiers based on block annotation policy.
 *
 * A single task may not be enough to completely align tiers, so
 * {@link ManagementTaskCoordinator} will keep instantiating new tasks
 * until no longer needed.
 */
public class AlignTask extends AbstractBlockManagementTask {
  private static final Logger LOG = LoggerFactory.getLogger(AlignTask.class);

  /**
   * Creates a new align task.
   *
   * @param blockStore the block store
   * @param metadataManager the meta manager
   * @param evictorView the evictor view
   * @param loadTracker the load tracker
   * @param executor the executor
   */
  public AlignTask(BlockStore blockStore, BlockMetadataManager metadataManager,
      BlockMetadataEvictorView evictorView, StoreLoadTracker loadTracker,
      ExecutorService executor) {
    super(blockStore, metadataManager, evictorView, loadTracker, executor);
  }

  @Override
  public BlockManagementTaskResult run() {
    LOG.debug("Running align task.");
    // Acquire align range from the configuration.
    // This will limit swap operations in a single run.
    final int alignRange =
        ServerConfiguration.getInt(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_RANGE);

    BlockManagementTaskResult result = new BlockManagementTaskResult();
    // Align each tier intersection by swapping blocks.
    for (Pair<BlockStoreLocation, BlockStoreLocation> intersection : mMetadataManager
        .getStorageTierAssoc().intersectionList()) {
      BlockStoreLocation tierUpLoc = intersection.getFirst();
      BlockStoreLocation tierDownLoc = intersection.getSecond();

      // Get list per tier that will be swapped for aligning the intersection.
      Pair<List<Long>, List<Long>> swapLists = mMetadataManager.getBlockIterator().getSwaps(
          tierUpLoc, BlockOrder.NATURAL, tierDownLoc, BlockOrder.REVERSE, alignRange,
          BlockOrder.REVERSE, (blockId) -> !mEvictorView.isBlockEvictable(blockId));

      Preconditions.checkArgument(swapLists.getFirst().size() == swapLists.getSecond().size());
      LOG.debug("Acquired {} block pairs to align tiers {} - {}", swapLists.getFirst().size(),
          tierUpLoc.tierAlias(), tierDownLoc.tierAlias());

      // Create exception handler to trigger swap-restore task when swap fails
      // due to insufficient reserved space.
      Consumer<Exception> excHandler = (e) -> {
        if (e instanceof WorkerOutOfSpaceException) {
          LOG.warn("Insufficient space for worker swap space, swap restore task called.");
          // Mark the need for running swap-space restoration task.
          TierManagementTaskProvider.setSwapRestoreRequired(true);
        }
      };

      // Execute swap transfers.
      BlockOperationResult tierResult =
          mTransferExecutor.executeTransferList(generateSwapTransferInfos(swapLists), excHandler);
      result.addOpResults(BlockOperationType.ALIGN_SWAP, tierResult);
    }
    return result;
  }

  private List<BlockTransferInfo> generateSwapTransferInfos(
      Pair<List<Long>, List<Long>> swapLists) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Generating transfer infos from swap lists.\n"
              + "Source list of size:{} : {}\nDestination list of size:{} : {}",
          swapLists.getFirst().size(),
          swapLists.getFirst().stream().map(Object::toString).collect(Collectors.joining(",")),
          swapLists.getSecond().size(),
          swapLists.getSecond().stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    // Function that is used to map blockId to <blockId,location> pair.
    Function<Long, Pair<Long, BlockStoreLocation>> blockToPairFunc = (blockId) -> {
      try {
        return new Pair(blockId, mEvictorView.getBlockMeta(blockId).getBlockLocation());
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Failed to find location of a block:{}. Error: {}", blockId, e);
        return new Pair(blockId, BlockStoreLocation.anyTier());
      }
    };

    // Generate an augmented block lists with locations.
    List<Pair<Long, BlockStoreLocation>> blockLocPairListSrc =
        swapLists.getFirst().stream().map(blockToPairFunc).collect(Collectors.toList());
    List<Pair<Long, BlockStoreLocation>> blockLocPairListDst =
        swapLists.getSecond().stream().map(blockToPairFunc).collect(Collectors.toList());

    // Sort augmented lists by location.
    // This will help to generate the buckets by a linear sweep.
    Comparator<Pair<Long, BlockStoreLocation>> comparator = (o1, o2) -> {
      BlockStoreLocation loc1 = o1.getSecond();
      BlockStoreLocation loc2 = o2.getSecond();
      int tierComp = loc1.tierAlias().compareTo(loc2.tierAlias());
      if (tierComp != 0) {
        return tierComp;
      } else {
        return loc1.dir() - loc2.dir();
      }
    };
    Collections.sort(blockLocPairListSrc, comparator);
    Collections.sort(blockLocPairListDst, comparator);

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Generated and sorted augmented swap lists.\n"
              + "Source list of size:{} :\n ->{}\nDestination list of size:{} :\n ->{}",
          blockLocPairListSrc.size(),
          blockLocPairListSrc.stream().map(Object::toString).collect(Collectors.joining("\n ->")),
          blockLocPairListDst.size(),
          blockLocPairListDst.stream().map(Object::toString).collect(Collectors.joining("\n ->")));
    }

    // Build transfer infos using the sorted locations.
    List<BlockTransferInfo> transferInfos = new ArrayList<>(blockLocPairListSrc.size());
    for (int i = 0; i < blockLocPairListSrc.size(); i++) {
      transferInfos.add(BlockTransferInfo.createSwap(blockLocPairListSrc.get(i).getSecond(),
          blockLocPairListSrc.get(i).getFirst(), blockLocPairListDst.get(i).getSecond(),
          blockLocPairListDst.get(i).getFirst()));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated {} swap transfers: \n ->{}", transferInfos.size(),
          transferInfos.stream().map(Object::toString).collect(Collectors.joining(",\n ->")));
    }
    return transferInfos;
  }
}
