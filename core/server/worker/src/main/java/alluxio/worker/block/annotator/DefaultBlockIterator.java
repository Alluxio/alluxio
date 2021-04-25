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

package alluxio.worker.block.annotator;

import alluxio.collections.ConcurrentHashSet;
import alluxio.collections.Pair;
import alluxio.worker.block.AbstractBlockStoreEventListener;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The default {@link BlockIterator} implementation that integrates with the
 * {@link BlockMetadataManager}.
 */
public class DefaultBlockIterator implements BlockIterator {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultBlockIterator.class);

  /** Map of sorted block set collections per directory. */
  private final Map<BlockStoreLocation, SortedBlockSet<BlockSortedField>> mPerDirOrderedSets;

  /** Used to update total order for offline sorting schemes. */
  private final Set<BlockStoreLocation> mUnorderedLocations;

  /** Configured block annotator class. */
  private final BlockAnnotator mBlockAnnotator;

  /** Underlying meta manager. */
  private final BlockMetadataManager mMetaManager;

  /** Block store delegate listener. */
  private BlockStoreEventListener mListener;

  /**
   * Creates default block iterator instance.
   *
   * @param metaManager meta manager
   * @param blockAnnotator block annotator
   */
  public DefaultBlockIterator(BlockMetadataManager metaManager, BlockAnnotator blockAnnotator) {
    mMetaManager = metaManager;
    mBlockAnnotator = blockAnnotator;

    mPerDirOrderedSets = new ConcurrentHashMap<>();
    mUnorderedLocations = new ConcurrentHashSet<>();
    mListener = new Listener();

    initialize();
  }

  /**
   * Initializes with the existing blocks.
   */
  private void initialize() {
    // Initialize sets per location.
    for (StorageTier tier : mMetaManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        mPerDirOrderedSets.put(dir.toBlockStoreLocation(), new SortedBlockSet());
        mUnorderedLocations.add(dir.toBlockStoreLocation());
      }
    }

    // Initialize with existing items.
    for (StorageTier tier : mMetaManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        BlockStoreLocation dirLocation = dir.toBlockStoreLocation();
        for (long blockId : dir.getBlockIds()) {
          blockUpdated(blockId, dirLocation);
        }
      }
    }
  }

  /**
   * Called by block-store event callbacks to update a block.
   */
  private void blockUpdated(long blockId, BlockStoreLocation location) {
    // Acquire the sorted-set for the target location.
    SortedBlockSet sortedSet = mPerDirOrderedSets.get(location);
    // Get new sort-field for the block.
    BlockSortedField sortedField =
        mBlockAnnotator.updateSortedField(blockId, sortedSet.getSortField(blockId));
    // Update the sorted-set.
    sortedSet.put(blockId, sortedField);

    // Mark the location for offline sort providers.
    if (!mBlockAnnotator.isOnlineSorter()) {
      mUnorderedLocations.add(location);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Block:{} updated at {} with {} blocks.", blockId, location, sortedSet.size());
    }
  }

  /**
   * Called by block-store event callbacks to remove a block.
   */
  private void blockRemoved(long blockId, BlockStoreLocation location) {
    // Acquire the sorted-set for the target location.
    SortedBlockSet sortedSet = mPerDirOrderedSets.get(location);
    // Remove from the sorted-set.
    sortedSet.remove(blockId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Block:{} removed from {} with {} blocks.", blockId, location, sortedSet.size());
    }
  }

  /**
   * Called by block-store event callbacks to move a block.
   */
  private void blockMoved(long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    // TODO(ggezer): Fix callback logic to not called for the same locations.
    if (!oldLocation.equals(newLocation)) {
      // Acquire the sorted-set for the block's current location.
      SortedBlockSet oldSortedSet = mPerDirOrderedSets.get(oldLocation);
      // Extract the old sorted-field and remove from source location's sorted-set.
      BlockSortedField oldSortField = oldSortedSet.getSortField(blockId);
      oldSortedSet.remove(blockId);
      // Update the destination sorted-set with the old sorted-field.
      SortedBlockSet newSortedSet = mPerDirOrderedSets.get(newLocation);
      newSortedSet.put(blockId, oldSortField);

      // Mark the location for offline annotators.
      if (!mBlockAnnotator.isOnlineSorter()) {
        mUnorderedLocations.add(newLocation);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Block: {} moved from {} with {} blocks to {} with {} blocks.",
            blockId, oldLocation, oldSortedSet.size(), newLocation, newSortedSet.size());
      }
    }
  }

  @Override
  public List<BlockStoreEventListener> getListeners() {
    return Arrays.asList(new BlockStoreEventListener[] {mListener});
  }

  @Override
  public Iterator<Long> getIterator(BlockStoreLocation location, BlockOrder order) {
    return Iterators.transform(getIteratorInternal(location, order), (pair) -> pair.getFirst());
  }

  @Override
  public List<Long> getIntersectionList(BlockStoreLocation srcLocation, BlockOrder srcOrder,
      BlockStoreLocation dstLocation, BlockOrder dstOrder, int intersectionWidth,
      BlockOrder intersectionOrder, Function<Long, Boolean> blockFilterFunc) {

    // Acquire source iterator based on given order.
    Iterator<Pair<Long, BlockSortedField>> srcIterator = getIteratorInternal(srcLocation, srcOrder);
    // Acquire destination iterator based on given order.
    Iterator<Pair<Long, BlockSortedField>> dstIterator = getIteratorInternal(dstLocation, dstOrder);

    // Build the list of entries with the entries that belongs to intersection.
    List<Pair<Long, BlockSortedField>> intersectionList = new ArrayList<>(intersectionWidth);
    while (srcIterator.hasNext() || dstIterator.hasNext()) {
      if (intersectionList.size() >= intersectionWidth) {
        break;
      }
      while (srcIterator.hasNext()) {
        Pair<Long, BlockSortedField> currPair = srcIterator.next();
        if (!blockFilterFunc.apply(currPair.getFirst())) {
          intersectionList.add(currPair);
          break;
        }
      }
      while (dstIterator.hasNext()) {
        Pair<Long, BlockSortedField> currPair = dstIterator.next();
        if (!blockFilterFunc.apply(currPair.getFirst())) {
          intersectionList.add(currPair);
          break;
        }
      }
    }

    // Sort the intersection list.
    intersectionList.sort((o1, o2) -> intersectionOrder.comparator().compare(
        o1.getSecond(), o2.getSecond()));
    // Return iterator off of sorted intersection list.
    return intersectionList.stream().map((kv) -> kv.getFirst()).collect(Collectors.toList());
  }

  @Override
  public Pair<List<Long>, List<Long>> getSwaps(BlockStoreLocation srcLocation, BlockOrder srcOrder,
      BlockStoreLocation dstLocation, BlockOrder dstOrder, int swapRange,
      BlockOrder intersectionOrder, Function<Long, Boolean> blockFilterFunc) {
    // Acquire source iterator based on given order.
    Iterator<Pair<Long, BlockSortedField>> srcIterator = getIteratorInternal(srcLocation, srcOrder);
    // Acquire destination iterator based on given order.
    Iterator<Pair<Long, BlockSortedField>> dstIterator = getIteratorInternal(dstLocation, dstOrder);

    // These lists are used to emulate block swapping.
    List<Pair<Long, BlockSortedField>> srcList = new ArrayList<>(swapRange);
    List<Pair<Long, BlockSortedField>> dstList = new ArrayList<>(swapRange);

    while (srcIterator.hasNext() && srcList.size() < swapRange) {
      Pair<Long, BlockSortedField> currPair = srcIterator.next();
      if (!blockFilterFunc.apply(currPair.getFirst())) {
        srcList.add(currPair);
      }
    }
    while (dstIterator.hasNext() && dstList.size() < swapRange) {
      Pair<Long, BlockSortedField> currPair = dstIterator.next();
      if (!blockFilterFunc.apply(currPair.getFirst())) {
        dstList.add(currPair);
      }
    }

    // Simulate swapping until both ends of the list are aligned.
    int swapLimit = Math.min(srcList.size(), dstList.size());
    int swapCount = 0;
    while (swapCount < swapLimit) {
      Pair<Long, BlockSortedField> srcItem = srcList.get(swapCount);
      Pair<Long, BlockSortedField> dstItem = dstList.get(swapCount);

      if (intersectionOrder.comparator().compare(srcItem.getSecond(), dstItem.getSecond()) <= 0) {
        break;
      }

      swapCount++;
    }

    return new Pair<>(
        srcList.subList(0, swapCount).stream().map((kv) -> kv.getFirst())
            .collect(Collectors.toList()),
        dstList.subList(0, swapCount).stream().map((kv) -> kv.getFirst())
            .collect(Collectors.toList()));
  }

  @Override
  public boolean aligned(BlockStoreLocation srcLocation, BlockStoreLocation dstLocation,
      BlockOrder order, Function<Long, Boolean> blockFilterFunc) {
    // Get source iterator with given source order.
    Iterator<Pair<Long, BlockSortedField>> srcIterator =
        getIteratorInternal(srcLocation, order);
    // Get destination iterator with the reverse of given source order.
    Iterator<Pair<Long, BlockSortedField>> dstIterator =
        getIteratorInternal(dstLocation, order.reversed());

    /**
     * Gets the first valid block entry from both locations based on given order.
     * Comparison of these entries will reveal if there is an overlap between locations.
     */
    Pair<Long, BlockSortedField> srcItem = null;
    while (srcIterator.hasNext()) {
      Pair<Long, BlockSortedField> currPair = srcIterator.next();
      if (!blockFilterFunc.apply(currPair.getFirst())) {
        srcItem = currPair;
        break;
      }
    }
    Pair<Long, BlockSortedField> dstItem = null;
    while (dstIterator.hasNext()) {
      Pair<Long, BlockSortedField> currPair = dstIterator.next();
      if (!blockFilterFunc.apply(currPair.getFirst())) {
        dstItem = currPair;
        break;
      }
    }

    return srcItem == null || dstItem == null
        || order.comparator().compare(srcItem.getSecond(), dstItem.getSecond()) >= 0;
  }

  /**
   * Internal utility to get sorted block iterator for a given location and order.
   */
  private Iterator<Pair<Long, BlockSortedField>> getIteratorInternal(BlockStoreLocation location,
      BlockOrder order) {
    // Gather each directory location that is under requested location.
    List<BlockStoreLocation> locations = mPerDirOrderedSets.keySet().stream()
        .filter((dirLocation) -> dirLocation.belongsTo(location)).collect(Collectors.toList());

    // For offline order providers, update total order for each dirty location.
    if (!mBlockAnnotator.isOnlineSorter()) {
      if (mUnorderedLocations.stream()
          .anyMatch((dirtyLocation) -> dirtyLocation.belongsTo(location))) {
        LOG.debug("Updating total order for directories that belong to {}", location);
        updateTotalOrder(locations);
      }
    }

    // Gather iterators per each directory based on given order.
    List<Iterator<Pair<Long, BlockSortedField>>> iteratorList =
        new ArrayList<>(mPerDirOrderedSets.size());
    for (BlockStoreLocation dirLocation : locations) {
      switch (order) {
        case NATURAL:
          iteratorList.add(mPerDirOrderedSets.get(dirLocation).getAscendingIterator());
          break;
        case REVERSE:
          iteratorList.add(mPerDirOrderedSets.get(dirLocation).getDescendingIterator());
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported sort order: %s", order.name()));
      }
    }

    // Return a merge-sorted iterator for gathered iterators.
    return Iterators.mergeSorted(iteratorList,
        Comparator.comparing(Pair::getSecond, order.comparator()));
  }

  /**
   * Offline order providers require full sweep of its range in order to provide
   * total order among all elements. (For instance, LRFU..)
   *
   * This will invoke order provider with the full list in order to satisfy this requirement.
   *
   * TODO(ggezer): Consider adding a new {@link BlockAnnotator} API to extract logical time.
   */
  private synchronized void updateTotalOrder(List<BlockStoreLocation> locations) {
    // No need if there is no unordered locations.
    if (mUnorderedLocations.isEmpty()) {
      return;
    }

    // Used to store and update old sorted fields for each location.
    List<Pair<Long, BlockSortedField>> updatedEntries = new ArrayList<>();

    for (BlockStoreLocation location : locations) {
      // Acquire sorted-block-set for the location.
      SortedBlockSet sortedSet = mPerDirOrderedSets.get(location);
      // Populate list for existing blocks on location.
      updatedEntries.clear();
      Iterator<Pair<Long, BlockSortedField>> locationIter = sortedSet.getAscendingIterator();
      while (locationIter.hasNext()) {
        updatedEntries.add(locationIter.next());
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating total order for {} with {} blocks.",
            location, updatedEntries.size());
      }

      // Invoke order provider to update fields all together.
      mBlockAnnotator.updateSortedFields(updatedEntries);

      // Reinsert updated values back to sorted-set.
      for (Pair<Long, BlockSortedField> updatedEntry : updatedEntries) {
        sortedSet.put(updatedEntry.getFirst(), updatedEntry.getSecond());
      }

      // Location is ordered now.
      mUnorderedLocations.remove(location);
    }
  }

  /**
   * Internal class used to forward block store events to this iterator implementation.
   */
  class Listener extends AbstractBlockStoreEventListener {
    @Override
    public void onAccessBlock(long sessionId, long blockId, BlockStoreLocation location) {
      blockUpdated(blockId, location);
    }

    @Override
    public void onCommitBlock(long sessionId, long blockId, BlockStoreLocation location) {
      blockUpdated(blockId, location);
    }

    @Override
    public void onRemoveBlock(long sessionId, long blockId, BlockStoreLocation location) {
      blockRemoved(blockId, location);
    }

    @Override
    public void onStorageLost(BlockStoreLocation dirLocation) {
      mPerDirOrderedSets.remove(dirLocation);
    }

    @Override
    public void onMoveBlockByClient(long sessionId, long blockId, BlockStoreLocation oldLocation,
        BlockStoreLocation newLocation) {
      blockMoved(blockId, oldLocation, newLocation);
    }

    @Override
    public void onMoveBlockByWorker(long sessionId, long blockId, BlockStoreLocation oldLocation,
        BlockStoreLocation newLocation) {
      blockMoved(blockId, oldLocation, newLocation);
    }
  }
}
