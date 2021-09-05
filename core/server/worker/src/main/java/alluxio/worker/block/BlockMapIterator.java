package alluxio.worker.block;

import alluxio.grpc.BlockIdList;
import alluxio.grpc.BlockStoreLocationProto;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.proto.journal.Block;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

// TODO(jiacheng): write a unit test for this class
public class BlockMapIterator implements Iterator<List<LocationBlockIdListEntry>> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMapIterator.class);

  public static final int BATCH_SIZE = 1000;

  Map<BlockStoreLocation, List<Long>> mBlockLocationMap;

  int mCurrentBlockLocationIndex;
  List<BlockStoreLocationProto> mBlockStoreLocationProtoList;


  Map<BlockStoreLocationProto, Iterator<Long>> mBlockLocationIteratorMap;

  Iterator<Long> currentIterator;

  Map<BlockStoreLocationProto, List<Long>> mTierToBlocks;

  // Keep a global counter of how many blocks have been traversed
  long mCounter = 0;

  // TODO(jiacheng): Lock while the constructor is running?
  BlockMapIterator(Map<BlockStoreLocation, List<Long>> blockLocationMap) {
    mBlockLocationMap = blockLocationMap;

    // TODO(jiacheng): Can this merging copy be avoided?
    mTierToBlocks = new HashMap<>();
    for (Map.Entry<BlockStoreLocation, List<Long>> entry : mBlockLocationMap.entrySet()) {
      BlockStoreLocation loc = entry.getKey();
      BlockStoreLocationProto locationProto = BlockStoreLocationProto.newBuilder()
              .setTierAlias(loc.tierAlias())
              .setMediumType(loc.mediumType())
              .build();
      if (mTierToBlocks.containsKey(locationProto)) {
        mTierToBlocks.get(locationProto).addAll(entry.getValue());
      } else {
        List<Long> blockList = new ArrayList<>(entry.getValue());
        mTierToBlocks.put(locationProto, blockList);
      }
    }
    LOG.info("Locations: {}", mTierToBlocks.keySet());

    // Initialize the iteration statuses
    mBlockStoreLocationProtoList = new ArrayList<>(mTierToBlocks.keySet());
    mCurrentBlockLocationIndex = 0; // Corresponding to index in mBlockLocationKeyList

    mBlockLocationIteratorMap = new HashMap<>();
    // TODO: make sure this entrySet is still the same with when generating mBlockStoreLocationProtoList
    for (Map.Entry<BlockStoreLocationProto, List<Long>> entry : mTierToBlocks.entrySet()) {
      mBlockLocationIteratorMap.put(entry.getKey(), entry.getValue().iterator());
    }

    // Initialize the iteration status
    currentIterator = mBlockLocationIteratorMap.get(mBlockStoreLocationProtoList.get(0));
  }

  @Override
  public boolean hasNext() {
    return mCurrentBlockLocationIndex < mBlockStoreLocationProtoList.size()
            || currentIterator.hasNext();
  }

  LocationBlockIdListEntry nextBatchFromTier(BlockStoreLocationProto currentLoc, Iterator<Long> currentIterator) {
    // Generate the next batch
    List<Long> blockIdBatch = new ArrayList<>(BATCH_SIZE); // this hint may be incorrect for new worker
    while (blockIdBatch.size() < BATCH_SIZE && currentIterator.hasNext()) {
      blockIdBatch.add(currentIterator.next());
      mCounter++;
    }
    LOG.info("{} blocks added to the batch", blockIdBatch.size());
    System.out.format("%s blocks added to the batch", blockIdBatch.size());

    // Initialize the LocationBlockIdListEntry
    BlockIdList blockIdList = BlockIdList.newBuilder().addAllBlockId(blockIdBatch).build();
    LocationBlockIdListEntry listEntry = LocationBlockIdListEntry.newBuilder()
            .setKey(currentLoc).setValue(blockIdList).build();
    return listEntry;
  }


  @Override
  public List<LocationBlockIdListEntry> next() {
    List<LocationBlockIdListEntry> result = new ArrayList<>();
    long currentCounter = mCounter;
    long targetCounter = currentCounter + BATCH_SIZE;

    while (mCounter < targetCounter) {
      // Find the BlockStoreLocation
      BlockStoreLocationProto currentLoc = mBlockStoreLocationProtoList.get(mCurrentBlockLocationIndex);
      Iterator<Long> currentIterator = mBlockLocationIteratorMap.get(currentLoc);

      // Generate the next batch
      // This method does NOT progress the pointers
      LocationBlockIdListEntry batchFromThisTier = nextBatchFromTier(currentLoc, currentIterator);
      result.add(batchFromThisTier);

      // Progress the iterator based on the break condition
      if (!currentIterator.hasNext()) {
//        System.out.println("Tier has been consumed");
//        LOG.info("Tier has been consumed.");
        // We keep filling in from the next tier
        // Update the pointer and continue
        mCurrentBlockLocationIndex++;
        if (mCurrentBlockLocationIndex >= mBlockStoreLocationProtoList.size()) {
          System.out.format("Finished all iterators. %s blocks iterated.%n", mCounter);
          LOG.info("Finished all iterators. {} blocks iterated.", mCounter);
          // We break out of the loop when the current iterator is exhausted
          // and all iterators have been exhausted
          LOG.info("Batch container {} tier entries", result.size());
          return result;
        }
        System.out.println("Continue with the next tier");
        LOG.info("Continue with the next tier {}", mBlockStoreLocationProtoList.get(mCurrentBlockLocationIndex));
        continue;
      } else {
//        System.out.format("Batch has been filled, counter is %s%n", mCounter);
//        LOG.info("Batch has been filled, now counter is {}", mCounter);
        return result;
      }
    }

    // Should never reach here
    return result;
  }
}
