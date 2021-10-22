package alluxio.worker.block;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.BlockIdList;
import alluxio.grpc.BlockStoreLocationProto;
import alluxio.grpc.LocationBlockIdListEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BlockMapIterator implements Iterator<List<LocationBlockIdListEntry>> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMapIterator.class);

  private AlluxioConfiguration mConf;
  private int mBatchSize;

  Map<BlockStoreLocation, List<Long>> mBlockLocationMap;

  int mCurrentBlockLocationIndex;
  List<BlockStoreLocationProto> mBlockStoreLocationProtoList;


  Map<BlockStoreLocationProto, Iterator<Long>> mBlockLocationIteratorMap;

  Iterator<Long> currentIterator;

  Map<BlockStoreLocationProto, List<Long>> mTierToBlocks;

  // Keep a global counter of how many blocks have been traversed
  int mCounter = 0;

  public BlockMapIterator(Map<BlockStoreLocation, List<Long>> blockLocationMap) {
    this(blockLocationMap, ServerConfiguration.global());
  }

  // TODO(jiacheng): report the number of iters so we have a CountDownLatch to count ACKs

  // TODO(jiacheng): Lock while the constructor is running?
  public BlockMapIterator(Map<BlockStoreLocation, List<Long>> blockLocationMap, AlluxioConfiguration conf) {
    mConf = conf;
    mBatchSize = mConf.getInt(PropertyKey.WORKER_REGISTER_BATCH_SIZE);

    LOG.info("Worker register batch size is {}", mBatchSize);
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

  LocationBlockIdListEntry nextBatchFromTier(BlockStoreLocationProto currentLoc, Iterator<Long> currentIterator,
                                             int spaceLeft) {
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
//          System.out.format("Finished all iterators. %s blocks iterated.%n", mCounter);
          LOG.info("Finished all iterators. {} blocks iterated.", mCounter);
          // We break out of the loop when the current iterator is exhausted
          // and all iterators have been exhausted
          LOG.info("Batch container {} tier entries", result.size());
          return result;
        }
        LOG.info("Continue with the next tier {}", mBlockStoreLocationProtoList.get(mCurrentBlockLocationIndex));
        continue;
      } else {
        LOG.info("Batch has been filled, now counter is {}", mCounter);
        return result;
      }
    }

    // Should never reach here
    return result;
  }
}
