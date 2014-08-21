package tachyon.master;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import tachyon.Constants;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;

/**
 * The structure to store a worker's information in master node.
 */
public class MasterWorkerInfo {

  /** Worker's address **/
  public final NetAddress ADDRESS;
  /** Capacity of worker in bytes **/
  private final long CAPACITY_BYTES;
  /** Start time of the worker in ms **/
  private final long START_TIME_MS;
  /** The id of the worker **/
  private long mId;
  /** Worker's used bytes **/
  private long mUsedBytes;
  /** Worker's last updated time in ms **/
  private long mLastUpdatedTimeMs;
  /** IDs of blocks the worker contains **/
  private Set<Long> mBlocks;
  /** IDs of blocks the worker should remove **/
  private Set<Long> mToRemoveBlocks;

  public MasterWorkerInfo(long id, NetAddress address, long capacityBytes) {
    mId = id;
    ADDRESS = address;
    CAPACITY_BYTES = capacityBytes;
    START_TIME_MS = System.currentTimeMillis();

    mUsedBytes = 0;
    mBlocks = new HashSet<Long>();
    mToRemoveBlocks = new HashSet<Long>();
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  /**
   * @return Generated {@link tachyon.thrift.ClientWorkerInfo} for this worker
   */
  public synchronized ClientWorkerInfo generateClientWorkerInfo() {
    ClientWorkerInfo ret = new ClientWorkerInfo();
    ret.id = mId;
    ret.address = ADDRESS;
    ret.lastContactSec =
        (int) ((CommonUtils.getCurrentMs() - mLastUpdatedTimeMs) / Constants.SECOND_MS);
    ret.state = "In Service";
    ret.capacityBytes = CAPACITY_BYTES;
    ret.usedBytes = mUsedBytes;
    ret.starttimeMs = START_TIME_MS;
    return ret;
  }

  /**
   * @return the worker's address.
   */
  public NetAddress getAddress() {
    return ADDRESS;
  }

  /**
   * @return the available space of the worker in bytes
   */
  public synchronized long getAvailableBytes() {
    return CAPACITY_BYTES - mUsedBytes;
  }

  /**
   * @return IDs of all blocks the worker contains.
   */
  public synchronized Set<Long> getBlocks() {
    return new HashSet<Long>(mBlocks);
  }

  /**
   * @return the capacity of the worker in bytes
   */
  public long getCapacityBytes() {
    return CAPACITY_BYTES;
  }

  /**
   * @return the ID of the worker
   */
  public synchronized long getId() {
    return mId;
  }

  /**
   * @return the last updated time of the worker in ms.
   */
  public synchronized long getLastUpdatedTimeMs() {
    return mLastUpdatedTimeMs;
  }

  /**
   * @return IDs of blocks the worker should remove
   */
  public synchronized List<Long> getToRemovedBlocks() {
    return new ArrayList<Long>(mToRemoveBlocks);
  }

  /**
   * @return used space of the worker in bytes
   */
  public synchronized long getUsedBytes() {
    return mUsedBytes;
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("MasterWorkerInfo(");
    sb.append(" ID: ").append(mId);
    sb.append(", ADDRESS: ").append(ADDRESS);
    sb.append(", TOTAL_BYTES: ").append(CAPACITY_BYTES);
    sb.append(", mUsedBytes: ").append(mUsedBytes);
    sb.append(", mAvailableBytes: ").append(CAPACITY_BYTES - mUsedBytes);
    sb.append(", mLastUpdatedTimeMs: ").append(mLastUpdatedTimeMs);
    sb.append(", mBlocks: [ ");
    for (long blockId : mBlocks) {
      sb.append(blockId).append(", ");
    }
    sb.append("] )");
    return sb.toString();
  }

  /**
   * Adds or removes a block from the worker
   * 
   * @param add
   *          true if to add, to remove otherwise.
   * @param blockId
   *          the ID of the block to be added or removed
   */
  public synchronized void updateBlock(boolean add, long blockId) {
    if (add) {
      mBlocks.add(blockId);
    } else {
      mBlocks.remove(blockId);
    }
  }

  /**
   * Adds or removes blocks from the worker
   * 
   * @param add
   *          true if to add, to remove otherwise.
   * @param blockIds
   *          IDs of the blocks to be added or removed
   */
  public synchronized void updateBlocks(boolean add, Collection<Long> blockIds) {
    if (add) {
      mBlocks.addAll(blockIds);
    } else {
      mBlocks.removeAll(blockIds);
    }
  }

  /**
   * Updates the last updated time of the worker in ms
   */
  public synchronized void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  /**
   * Adds or removes a block from the to-be-removed blocks set of the worker.
   * 
   * @param add
   *          true if to add, to remove otherwise.
   * @param blockId
   *          the ID of the block to be added or removed
   */
  public synchronized void updateToRemovedBlock(boolean add, long blockId) {
    if (add) {
      if (mBlocks.contains(blockId)) {
        mToRemoveBlocks.add(blockId);
      }
    } else {
      mToRemoveBlocks.remove(blockId);
    }
  }

  /**
   * Adds or removes blocks from the to-be-removed blocks set of the worker.
   * 
   * @param add
   *          true if to add, to remove otherwise.
   * @param blockIds
   *          IDs of blocks to be added or removed
   */
  public synchronized void updateToRemovedBlocks(boolean add, Collection<Long> blockIds) {
    for (long blockId : blockIds) {
      updateToRemovedBlock(add, blockId);
    }
  }

  /**
   * Set the used space of the worker in bytes.
   * 
   * @param usedBytes
   *          the used space in bytes
   */
  public synchronized void updateUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
  }
}