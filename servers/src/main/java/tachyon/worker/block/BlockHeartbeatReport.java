package tachyon.worker.block;

import java.util.List;
import java.util.Map;

import tachyon.worker.WorkerReport;

/**
 * Container for the delta information in each worker to master heartbeat.
 */
// TODO: This may be better as a thrift object
public class BlockHeartbeatReport extends WorkerReport {
  private final List<Long> mUsedBytesOnTiers;
  private final List<Long> mRemovedBlocks;
  private final Map<Long, List<Long>> mAddedBlocks;

  public BlockHeartbeatReport(List<Long> usedBytesOnTier, List<Long> removedBlocks,
      Map<Long, List<Long>> addedBlocks) {
    mUsedBytesOnTiers = usedBytesOnTier;
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
