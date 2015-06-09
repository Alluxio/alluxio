package tachyon.worker;

import java.util.List;
import java.util.Map;

/**
 * Represents the data the CoreWorker will send to the master in its periodic heartbeat.
 */
// TODO: Make this a thrift object?
public class BlockWorkerReport extends WorkerReport {
  private final long mWorkerId;
  private final List<Long> mUsedBytesOnTiers;
  private final List<Long> mRemovedBlocks;
  private final Map<Long, List<Long>> mAddedBlocks;

  public BlockWorkerReport(long workerId, List<Long> usedBytesOnTiers, List<Long> removedBlocks,
      Map<Long, List<Long>> addedBlocks) {
    mWorkerId = workerId;
    mUsedBytesOnTiers = usedBytesOnTiers;
    mRemovedBlocks = removedBlocks;
    mAddedBlocks = addedBlocks;
  }

  public Map<Long, List<Long>> getAddedBlocks() {
    return mAddedBlocks;
  }

  public List<Long> getRemovedBlocks() {
    return mRemovedBlocks;
  }

  public List<Long> getUsedBytesOnTiers() {
    return mUsedBytesOnTiers;
  }

  public long getWorkerId() {
    return mWorkerId;
  }
}
