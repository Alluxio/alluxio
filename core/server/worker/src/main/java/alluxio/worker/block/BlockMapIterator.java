package alluxio.worker.block;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.BlockIdList;
import alluxio.grpc.BlockStoreLocationProto;
import alluxio.grpc.LocationBlockIdListEntry;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BlockMapIterator implements Iterator<List<LocationBlockIdListEntry>> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMapIterator.class);

  private AlluxioConfiguration mConf;
  private int mBatchSize;
  private int mBlockCount;

  // Keep the order of iteration
  List<BlockStoreLocationProto> mBlockStoreLocationProtoList;
  Map<BlockStoreLocationProto, Iterator<Long>> mBlockLocationIteratorMap;
  // Iteration states
  int mCurrentBlockLocationIndex;
  Iterator<Long> currentIterator;
  // Keep a global counter of how many blocks have been traversed
  int mCounter = 0;

  public BlockMapIterator(Map<BlockStoreLocation, List<Long>> blockLocationMap) {
    this(blockLocationMap, ServerConfiguration.global());
  }

  public BlockMapIterator(Map<BlockStoreLocation, List<Long>> blockLocationMap, AlluxioConfiguration conf) {
    mConf = conf;
    mBatchSize = mConf.getInt(PropertyKey.WORKER_REGISTER_STREAM_BATCH_SIZE);

    LOG.info("Worker register batch size is {}", mBatchSize);

    // The worker will merge the block lists from dirs on the same tier
    // because the master only wants one list for each tier.
    // TODO(jiacheng): try to avoid this merge copy
    Map<BlockStoreLocationProto, List<Long>> tierToBlocks = new HashMap<>();
    for (Map.Entry<BlockStoreLocation, List<Long>> entry : blockLocationMap.entrySet()) {
      BlockStoreLocation loc = entry.getKey();
      BlockStoreLocationProto locationProto = BlockStoreLocationProto.newBuilder()
              .setTierAlias(loc.tierAlias())
              .setMediumType(loc.mediumType())
              .build();
      if (tierToBlocks.containsKey(locationProto)) {
        tierToBlocks.get(locationProto).addAll(entry.getValue());
      } else {
        List<Long> blockList = new ArrayList<>(entry.getValue());
        tierToBlocks.put(locationProto, blockList);
      }
    }
    LOG.info("Locations: {}", tierToBlocks.keySet());

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
      currentIterator = Collections.emptyIterator();
    } else {
      currentIterator = mBlockLocationIteratorMap.get(mBlockStoreLocationProtoList.get(0));
    }
  }

  @Override
  public boolean hasNext() {
    // TODO(jiacheng): test multiple tiers/dirs are all empty
    return currentIterator.hasNext()
        // at least 1 request if all tiers are empty
        || mCurrentBlockLocationIndex < mBlockStoreLocationProtoList.size();
  }

  private LocationBlockIdListEntry nextBatchFromTier(
      BlockStoreLocationProto currentLoc, Iterator<Long> currentIterator, int spaceLeft) {
    // Generate the next batch
    List<Long> blockIdBatch = new ArrayList<>(spaceLeft); // this hint may be incorrect for new worker
    while (blockIdBatch.size() < spaceLeft && currentIterator.hasNext()) {
      blockIdBatch.add(currentIterator.next());
      mCounter++;
    }

    // Initialize the LocationBlockIdListEntry
    BlockIdList blockIdList = BlockIdList.newBuilder().addAllBlockId(blockIdBatch).build();
    LocationBlockIdListEntry listEntry = LocationBlockIdListEntry.newBuilder()
        .setKey(currentLoc).setValue(blockIdList).build();
    return listEntry;
  }

  @Override
  public List<LocationBlockIdListEntry> next() {
    List<LocationBlockIdListEntry> result = new ArrayList<>();
    int currentCounter = mCounter;
    int targetCounter = currentCounter + mBatchSize;

    while (mCounter < targetCounter) {
      // Find the BlockStoreLocation
      BlockStoreLocationProto currentLoc = mBlockStoreLocationProtoList.get(mCurrentBlockLocationIndex);
      Iterator<Long> currentIterator = mBlockLocationIteratorMap.get(currentLoc);

      // Generate the next batch
      // This method does NOT progress the pointers
      int spaceLeft = targetCounter - mCounter;
      LocationBlockIdListEntry batchFromThisTier = nextBatchFromTier(currentLoc, currentIterator, spaceLeft);
      result.add(batchFromThisTier);

      // Progress the iterator based on the break condition
      if (!currentIterator.hasNext()) {
        // We keep filling in from the next tier
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

  public int getBlockCount() {
    return mBlockCount;
  }

  public int getBatchCount() {
    return (int) Math.ceil(mBlockCount / (double) mBatchSize);
  }
}
