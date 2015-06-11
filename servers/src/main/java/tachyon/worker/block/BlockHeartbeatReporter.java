package tachyon.worker.block;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tachyon.worker.BlockStoreLocation;

/**
 * Represents the data the BlockWorker will send to the master in its periodic heartbeat.
 */
public class BlockHeartbeatReporter implements BlockMetaEventListener {

  private List<Long> mRemovedBlocks;
  private Map<Long, List<Long>> mAddedBlocks;

  public BlockHeartbeatReporter() {
    mRemovedBlocks = new ArrayList<Long>(100);
    mAddedBlocks = new HashMap<Long, List<Long>>();
  }

  public void preCommitBlock(long userId, long blockId) {
    // Do nothing
  }

  public void postCommitBlock(long userId, long blockId) {

  }

  public void preMoveBlock(long userId, long blockId, BlockStoreLocation newLocation) {
    // Do nothing
  }

  public void postMoveBlock(long userId, long blockId, BlockStoreLocation newLocation) {

  }

  public void preRemoveBlock(long userId, long blockId) {
    // Do nothing
  }

  public void postRemoveBlock(long userId, long blockId) {

  }
}
