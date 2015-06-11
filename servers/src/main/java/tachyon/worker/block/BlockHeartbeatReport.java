package tachyon.worker.block;

import java.util.List;
import java.util.Map;

/**
 * Container for the delta information in each worker to master heartbeat.
 */
public class BlockHeartbeatReport {
  /** Map of storage dirs to list of blocks added in the last heartbeat period */
  private final Map<Long, List<Long>> mAddedBlocks;
  /** List of blocks removed in the last heartbeat period */
  private final List<Long> mRemovedBlocks;

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
