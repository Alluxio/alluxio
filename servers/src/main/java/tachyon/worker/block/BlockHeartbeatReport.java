package tachyon.worker.block;

import java.util.List;
import java.util.Map;

import tachyon.worker.WorkerReport;

/**
 * Container for the delta information in each worker to master heartbeat.
 */
public class BlockHeartbeatReport extends WorkerReport {
  private final List<Long> mRemovedBlocks;
  private final Map<Long, List<Long>> mAddedBlocks;

  public BlockHeartbeatReport(Map<Long, List<Long>> addedBlocks, List<Long> removedBlocks) {
    mRemovedBlocks = removedBlocks;
    mAddedBlocks = addedBlocks;
  }

  public Map<Long, List<Long>> getAddedBlocks() {
    return mAddedBlocks;
  }

  public List<Long> getRemovedBlocks() {
    return mRemovedBlocks;
  }
}
