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

import alluxio.collections.Pair;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * An interface to provide various block iteration utilities for eviction
 * and various management tasks.
 */
public interface BlockIterator {
  /**
   * Gets an iterator of block-Ids.
   *
   * @param location location to iterate for blocks
   * @param order order of blocks
   * @return an iterator
   */
  Iterator<Long> getIterator(BlockStoreLocation location, BlockOrder order);

  /**
   * Used to get blocks within an intersection of locations in sorted order.
   *
   * It receives a filter using which certain blocks could be excluded from intersection iterator.
   *
   * @param srcLocation source location
   * @param srcOrder order for the source location
   * @param dstLocation destination location
   * @param dstOrder order for the destination location
   * @param intersectionWidth width of intersection
   * @param intersectionOrder the order of intersected blocks
   * @param blockFilterFunc a filter for blocks
   * @return list of blocks in intersection
   */
  List<Long> getIntersectionList(BlockStoreLocation srcLocation, BlockOrder srcOrder,
      BlockStoreLocation dstLocation, BlockOrder dstOrder, int intersectionWidth,
      BlockOrder intersectionOrder, Function<Long, Boolean> blockFilterFunc);

  /**
   * Used to list of blocks, that if swapped, could eliminate overlap between two tiers.
   *
   * It receives a filter using which certain blocks could be excluded from intersection iterator.
   *
   * @param srcLocation source location
   * @param srcOrder order for the source location
   * @param dstLocation destination location
   * @param dstOrder order for the destination location
   * @param swapRange per-tier range of swap
   * @param intersectionOrder the order of intersected blocks
   * @param blockFilterFunc a filter for blocks
   * @return 2 list of blocks to swap
   */
  Pair<List<Long>, List<Long>> getSwaps(BlockStoreLocation srcLocation, BlockOrder srcOrder,
      BlockStoreLocation dstLocation, BlockOrder dstOrder, int swapRange,
      BlockOrder intersectionOrder, Function<Long, Boolean> blockFilterFunc);

  /**
   * Used to detect presence of an overlap between two locations.
   * Overlap will be decided based on configured eviction order policy.
   *
   * It receives a filter using which certain blocks could be excluded from overlap decision.
   *
   * @param srcLocation source location
   * @param dstLocation destination location
   * @param order order of comparison between locations
   * @param blockFilterFunc a filter for blocks
   * @return {@code true} if tiers are aligned
   */
  boolean aligned(BlockStoreLocation srcLocation, BlockStoreLocation dstLocation, BlockOrder order,
      Function<Long, Boolean> blockFilterFunc);

  /**
   * Used to acquire and register listeners that are used by this iterator.
   *
   * @return list of event listeners
   */
  List<BlockStoreEventListener> getListeners();
}
