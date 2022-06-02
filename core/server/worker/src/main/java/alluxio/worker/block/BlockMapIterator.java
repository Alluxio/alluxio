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

package alluxio.worker.block;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.BlockIdList;
import alluxio.grpc.BlockStoreLocationProto;
import alluxio.grpc.LocationBlockIdListEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Each iteration returns a list of {@link LocationBlockIdListEntry} which consists of
 * blocks for one {@link alluxio.grpc.RegisterWorkerPRequest}.
 * The number of blocks included in one iteration is specified by
 * {@code PropertyKey.WORKER_REGISTER_STREAM_BATCH_SIZE}.
 */
public class BlockMapIterator implements Iterator<List<LocationBlockIdListEntry>> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMapIterator.class);

  private final int mBatchSize;
  private final int mBlockCount;
  // Keeps the order of iteration
  private final List<BlockStoreLocationProto> mBlockStoreLocationProtoList;
  private final Map<BlockStoreLocationProto, Iterator<Long>> mBlockLocationIteratorMap;
  // Iteration states
  private int mCurrentBlockLocationIndex;
  private final Iterator<Long> mCurrentIterator;
  // A global counter of how many blocks have been traversed
  private int mCounter = 0;

  /**
   * Constructor.
   *
   * @param blockLocationMap the block lists for each location
   */
  public BlockMapIterator(Map<BlockStoreLocation, List<Long>> blockLocationMap) {
    this(blockLocationMap, ServerConfiguration.global());
  }

  /**
   * Constructor.
   *
   * @param blockLocationMap the block lists for each location
   * @param conf configuration properties
   */
  public BlockMapIterator(
      Map<BlockStoreLocation, List<Long>> blockLocationMap, AlluxioConfiguration conf) {
    mBatchSize = conf.getInt(PropertyKey.WORKER_REGISTER_STREAM_BATCH_SIZE);
    LOG.info("Worker register stream batchSize={}", mBatchSize);

    // The worker will merge the block lists from dirs on the same tier
    // because the master only wants one list for each tier.
    // TODO(jiacheng): try to avoid this merge copy
    Map<BlockStoreLocationProto, List<Long>> tierToBlocks = new HashMap<>();
    for (Map.Entry<BlockStoreLocation, List<Long>> entry : blockLocationMap.entrySet()) {
      BlockStoreLocation loc = entry.getKey();
      BlockStoreLocationProto locationProto = BlockStoreLocationProto.newBuilder()
          .setTierAlias(loc.tierAlias()).setMediumType(loc.mediumType()).build();
      if (tierToBlocks.containsKey(locationProto)) {
        tierToBlocks.get(locationProto).addAll(entry.getValue());
      } else {
        List<Long> blockList = new ArrayList<>(entry.getValue());
        tierToBlocks.put(locationProto, blockList);
      }
    }
    LOG.debug("Found blocks for tiers: {}", tierToBlocks.keySet());

    // Initialize the iteration statuses
    mBlockStoreLocationProtoList = new ArrayList<>(tierToBlocks.keySet());
    mCurrentBlockLocationIndex = 0; // Corresponding to index in mBlockLocationKeyList

    // Find the iterator of each tier and calculate total block count
    mBlockLocationIteratorMap = new HashMap<>();
    int totalCount = 0;
    for (Map.Entry<BlockStoreLocationProto, List<Long>> entry : tierToBlocks.entrySet()) {
      totalCount += entry.getValue().size();
      mBlockLocationIteratorMap.put(entry.getKey(), entry.getValue().iterator());
    }
    mBlockCount = totalCount;

    // Initialize the iteration status
    if (mBlockStoreLocationProtoList.size() == 0) {
      mCurrentIterator = Collections.emptyIterator();
    } else {
      mCurrentIterator = mBlockLocationIteratorMap.get(mBlockStoreLocationProtoList.get(0));
    }
  }

  @Override
  public boolean hasNext() {
    return mCurrentIterator.hasNext()
        || mCurrentBlockLocationIndex < mBlockStoreLocationProtoList.size();
  }

  private LocationBlockIdListEntry nextBatchFromTier(
      BlockStoreLocationProto currentLoc, Iterator<Long> currentIterator, int spaceLeft) {
    // Generate the next batch
    List<Long> blockIdBatch = new ArrayList<>(spaceLeft);
    while (blockIdBatch.size() < spaceLeft && currentIterator.hasNext()) {
      blockIdBatch.add(currentIterator.next());
      mCounter++;
    }
    BlockIdList blockIdList = BlockIdList.newBuilder().addAllBlockId(blockIdBatch).build();
    return LocationBlockIdListEntry.newBuilder()
        .setKey(currentLoc).setValue(blockIdList).build();
  }

  @Override
  public List<LocationBlockIdListEntry> next() {
    List<LocationBlockIdListEntry> result = new ArrayList<>();
    int currentCounter = mCounter;
    int targetCounter = currentCounter + mBatchSize;

    while (mCounter < targetCounter) {
      // Find the BlockStoreLocation
      BlockStoreLocationProto currentLoc =
          mBlockStoreLocationProtoList.get(mCurrentBlockLocationIndex);
      Iterator<Long> currentIterator = mBlockLocationIteratorMap.get(currentLoc);

      // Generate the next batch
      // This method does NOT progress the pointers
      int spaceLeft = targetCounter - mCounter;
      LocationBlockIdListEntry batchFromThisTier =
          nextBatchFromTier(currentLoc, currentIterator, spaceLeft);
      result.add(batchFromThisTier);

      // Progress the iterator based on the break condition
      if (!currentIterator.hasNext()) {
        // We keep filling in using the next tier
        mCurrentBlockLocationIndex++;
        if (mCurrentBlockLocationIndex >= mBlockStoreLocationProtoList.size()) {
          // We break out of the loop when the current iterator is exhausted
          // and all iterators have been exhausted
          return result;
        }
        continue;
      } else {
        return result;
      }
    }

    // Should never reach here
    return result;
  }

  /**
   * Gets the number of batches.
   *
   * @return the count
   */
  public int getBatchCount() {
    return (int) Math.ceil(mBlockCount / (double) mBatchSize);
  }
}
