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

package alluxio.worker.block.management;

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
 * A BlockStore management task that is to swap blocks between tiers in order to eliminate overlaps
 * according to eviction order scheme.
 *
 * A single task may not be enough to completely eliminate the overlap, so
 * {@link ManagementTaskCoordinator} will keep instantiating new swap tasks
 * until no longer needed.
 */
public class TierSwapTask extends AbstractBlockManagementTask {
  private static final Logger LOG = LoggerFactory.getLogger(TierSwapTask.class);

  /**
   * Creates a new swap task.
   *
   * @param blockStore the block store
   * @param metadataManager the meta manager
   * @param evictorView the evictor view
   * @param loadTracker the load tracker
   * @param executor the executor
   */
  public TierSwapTask(BlockStore blockStore, BlockMetadataManager metadataManager,
      BlockMetadataEvictorView evictorView, StoreLoadTracker loadTracker,
      ExecutorService executor) {
    super(blockStore, metadataManager, evictorView, loadTracker, executor);
  }

  @Override
  public void run() {
    // Acquire swap range from the configuration.
    // This will limit swap operations in a single run.
    final int swapRange = ServerConfiguration.getInt(PropertyKey.WORKER_MANAGEMENT_TIER_SWAP_RANGE);

    // Iterate each tier intersection and avoid overlaps by swapping blocks.
    for (Pair<BlockStoreLocation, BlockStoreLocation> intersection : mMetadataManager
        .getStorageTierAssoc().intersectionList()) {
      BlockStoreLocation tierUpLocation = intersection.getFirst();
      BlockStoreLocation tierDownLocation = intersection.getSecond();

      // Get blocks lists for swapping that will be swapped to eliminate overlap.
      Pair<List<Long>, List<Long>> swapLists = mMetadataManager.getBlockIterator().getSwaps(
          tierUpLocation, BlockOrder.Natural, tierDownLocation, BlockOrder.Reverse, swapRange,
          BlockOrder.Reverse, (blockId) -> !mEvictorView.isBlockEvictable(blockId));
      Preconditions.checkArgument(swapLists.getFirst().size() == swapLists.getSecond().size());

      // Create exception handler to trigger swap-restore task when swap fails due to space.
      Consumer<Exception> excHandler = (e) -> {
        if (e instanceof WorkerOutOfSpaceException) {
          // Mark the need for running swap-space restoration task.
          TierManagementTaskProvider.setSwapRestoreRequired(true);
        }
      };

      // Execute transfers.
      mTransferExecutor.executeTransferList(generateSwapTransferInfos(swapLists), excHandler);
    }
  }

  private List<BlockTransferInfo> generateSwapTransferInfos(
      Pair<List<Long>, List<Long>> swapLists) {
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

    // Build transfer infos off sorted locations.
    List<BlockTransferInfo> transferInfos = new ArrayList<>(blockLocPairListSrc.size());
    for (int i = 0; i < blockLocPairListSrc.size(); i++) {
      transferInfos.add(BlockTransferInfo.createSwap(blockLocPairListSrc.get(i).getSecond(),
          blockLocPairListSrc.get(i).getFirst(), blockLocPairListDst.get(i).getSecond(),
          blockLocPairListDst.get(i).getFirst()));
    }

    return transferInfos;
  }
}
