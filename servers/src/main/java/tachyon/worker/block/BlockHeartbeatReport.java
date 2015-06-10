package tachyon.worker.block;

import java.util.List;
import java.util.Map;

import tachyon.worker.WorkerReport;

/**
 * Represents the data the BlockWorker will send to the master in its periodic heartbeat.
 */
// TODO: Make this a thrift object?
public class BlockHeartbeatReport extends WorkerReport {
  private final List<Long> mUsedBytesOnTiers;
  private final List<Long> mRemovedBlocks;
  private final Map<Long, List<Long>> mAddedBlocks;

  public BlockHeartbeatReport(List<Long> usedBytesOnTiers, List<Long> removedBlocks,
      Map<Long, List<Long>> addedBlocks) {
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
}
