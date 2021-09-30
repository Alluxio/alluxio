/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.block.meta;

import alluxio.Constants;
import alluxio.StorageTierAssoc;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.client.block.options.GetWorkerReportOptions.WorkerInfoField;
import alluxio.grpc.StorageList;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.resource.LockResource;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class embeds all metadata for an Alluxio worker.
 * This class is not thread safe, so external locking is required.
 *
 * There are multiple locks in this object, each guarding a group of metadata.
 *
 * The metadata fields are separated into a few different groups.
 * Each group has its corresponding locking mechanism.
 * The {@link MasterWorkerInfo} has the following groups of metadata:
 *  1. Metadata like ID, address etc., represented by a {@link StaticWorkerMeta} object.
 *     This group is thread safe, meaning no locking is required.
 *  2. Worker last updated timestamp. This is thread safe, meaning no locking is required.
 *  3. Worker register status. This is guarded by a {@link StampedLock#asReadWriteLock()}.
 *  4. Worker resource usage, represented by a {@link WorkerUsageMeta} object.
 *     This is guarded by a {@link StampedLock#asReadWriteLock()}.
 *  5. Worker block lists, including the present blocks and blocks to be removed from the worker.
 *     This is guarded by a {@link StampedLock#asReadWriteLock()}.
 *
 * When accessing certain fields in this object, external locking is required.
 * As listed above, group 1 and 2 are thread safe and do not require external locking.
 * Group 3, 4, and 5 require external locking.
 *
 * Locking can be done with {@link #lockWorkerMeta(EnumSet, boolean)}.
 * This method returns a {@link LockResource} which can be managed by try-finally.
 * Internally, {@link WorkerMetaLock} is used to manage the corresponding internal locks
 * with an order and unlocking them properly on close.
 *
 * If the fields are only read, shared locks should be acquired and released as below:
 * <blockquote><pre>
 *   try (LockResource r = lockWorkerMeta(
 *       EnumSet.of(WorkerMetaLockSection.USAGE), true)) {
 *     ...
 *   }
 * </pre></blockquote>
 *
 * If the fields are updated, exclusive locks should be acquired and released as below:
 * <blockquote><pre>
 *   try (LockResource r = lockWorkerMeta(
 *       EnumSet.of(
 *           WorkerMetaLockSection.STATUS,
 *           WorkerMetaLockSection.USAGE,
 *           WorkerMetaLockSection.BLOCKS)), true)) {
 *     ...
 *   }
 * </pre></blockquote>
 *
 * The locks are internally {@link StampedLock} which are NOT reentrant!
 * We chose {@link StampedLock} instead of {@link ReentrantReadWriteLock}
 * because the latter does not allow the write lock to be grabbed in one thread
 * but later released in another thread.
 * This is undesirable because when the worker registers in a stream, the thread
 * that handles the 1st message will acquire the lock and the thread that handles
 * the complete signal will be responsible for releasing the lock.
 * In the current gRPC architecture it is impossible to enforce the two threads
 * to be the same.
 *
 * Because then locks are not reentrant, you must be extra careful NOT to
 * acquire the lock while holding, because that will result in a deadlock!
 * This is especially the case for write locks.
 *
 * The current write lock holders include the following:
 * 1. In {@link DefaultBlockMaster}, the methods related to worker register/heartbeat,
 *    and block removal/commit.
 * 2. In {@link alluxio.master.block.WorkerRegisterContext},
 *    to write locks are held throughout the lifecycle.
 * 3. In {@link DefaultBlockMaster.LostWorkerDetectionHeartbeatExecutor#heartbeat()}
 */
@NotThreadSafe
public final class MasterWorkerInfo {
  private static final Logger LOG = LoggerFactory.getLogger(MasterWorkerInfo.class);
  private static final String LIVE_WORKER_STATE = "In Service";
  private static final String LOST_WORKER_STATE = "Out of Service";

  private static final EnumSet<WorkerInfoField> USAGE_INFO_FIELDS =
      EnumSet.of(WorkerInfoField.WORKER_CAPACITY_BYTES,
        WorkerInfoField.WORKER_CAPACITY_BYTES_ON_TIERS,
        WorkerInfoField.WORKER_USED_BYTES,
        WorkerInfoField.WORKER_USED_BYTES_ON_TIERS);

  /** Worker's last updated time in ms. */
  private final AtomicLong mLastUpdatedTimeMs;
  /** Worker metadata, this field is thread safe. */
  private final StaticWorkerMeta mMeta;

  /** If true, the worker is considered registered. */
  @GuardedBy("mStatusLock")
  public boolean mIsRegistered;
  /** Locks the worker register status. */
  private final ReadWriteLock mStatusLock;

  /** Worker usage data. */
  @GuardedBy("mUsageLock")
  private final WorkerUsageMeta mUsage;
  /** Locks the worker usage data. */
  private final ReadWriteLock mUsageLock;

  /** Ids of blocks the worker contains. */
  @GuardedBy("mBlockListLock")
  private Set<Long> mBlocks;
  /** Ids of blocks the worker should remove. */
  @GuardedBy("mBlockListLock")
  private final Set<Long> mToRemoveBlocks;
  /** Locks the 2 block sets above. */
  private final ReadWriteLock mBlockListLock;

  /** Stores the mapping from WorkerMetaLockSection to the lock. */
  private final Map<WorkerMetaLockSection, ReadWriteLock> mLockTypeToLock;

  /**
   * Creates a new instance of {@link MasterWorkerInfo}.
   *
   * @param id the worker id to use
   * @param address the worker address to use
   */
  public MasterWorkerInfo(long id, WorkerNetAddress address) {
    mMeta = new StaticWorkerMeta(id, address);
    mUsage = new WorkerUsageMeta();
    mBlocks = new HashSet<>();
    mToRemoveBlocks = new HashSet<>();
    mLastUpdatedTimeMs = new AtomicLong(CommonUtils.getCurrentMs());

    // Init all locks
    mStatusLock = new StampedLock().asReadWriteLock();
    mUsageLock = new StampedLock().asReadWriteLock();
    mBlockListLock = new StampedLock().asReadWriteLock();
    mLockTypeToLock = ImmutableMap.of(
        WorkerMetaLockSection.STATUS, mStatusLock,
        WorkerMetaLockSection.USAGE, mUsageLock,
        WorkerMetaLockSection.BLOCKS, mBlockListLock);
  }

  /**
   * Marks the worker as registered, while updating all of its metadata.
   * Write locks on {@link MasterWorkerInfo#mStatusLock}, {@link MasterWorkerInfo#mUsageLock}
   * and {@link MasterWorkerInfo#mBlockListLock} are required.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with all three lock types specified:
   *
   * <blockquote><pre>
   *   try (LockResource r = worker.lockWorkerMeta(EnumSet.of(
   *       WorkerMetaLockSection.STATUS,
   *       WorkerMetaLockSection.USAGE,
   *       WorkerMetaLockSection.BLOCKS), false)) {
   *     register(...);
   *   }
   * </pre></blockquote>
   *
   * @param globalStorageTierAssoc global mapping between storage aliases and ordinal position
   * @param storageTierAliases list of storage tier aliases in order of their position in the
   *        hierarchy
   * @param totalBytesOnTiers mapping from storage tier alias to total bytes
   * @param usedBytesOnTiers mapping from storage tier alias to used byes
   * @param blocks set of block ids on this worker
   * @return A Set of blocks removed (or lost) from this worker
   */
  public Set<Long> register(final StorageTierAssoc globalStorageTierAssoc,
      final List<String> storageTierAliases, final Map<String, Long> totalBytesOnTiers,
      final Map<String, Long> usedBytesOnTiers, final Set<Long> blocks) {
    mUsage.updateUsage(globalStorageTierAssoc, storageTierAliases,
            totalBytesOnTiers, usedBytesOnTiers);

    Set<Long> removedBlocks;
    if (mIsRegistered) {
      // This is a re-register of an existing worker. Assume the new block ownership data is more
      // up-to-date and update the existing block information.
      LOG.info("re-registering an existing workerId: {}", mMeta.mId);

      // Compute the difference between the existing block data, and the new data.
      removedBlocks = Sets.difference(mBlocks, blocks);
    } else {
      removedBlocks = Collections.emptySet();
    }

    // Set the new block information.
    mBlocks = blocks;

    mIsRegistered = true;
    return removedBlocks;
  }

  /**
   * Adds a block to the worker.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#BLOCKS} specified.
   * An exclusive lock is required.
   *
   * @param blockId the id of the block to be added
   */
  public void addBlock(long blockId) {
    mBlocks.add(blockId);
    // This step is added because in the beginning of a stream register
    // we mark all blocks to be removed
    mToRemoveBlocks.remove(blockId);
  }

  /**
   * Removes a block from the worker.
   * This is typically called when we know the block has been removed from the worker.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#BLOCKS} specified.
   * An exclusive lock is required.
   *
   * @param blockId the id of the block to be removed
   */
  public void removeBlockFromWorkerMeta(long blockId) {
    mBlocks.remove(blockId);
    mToRemoveBlocks.remove(blockId);
  }

  /**
   * Remove the block from the worker metadata and add to the to-remove list.
   * The next worker heartbeat will issue the remove command to the worker
   * so the block is deleted later.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#BLOCKS} specified.
   * An exclusive lock is required.
   *
   * @param blockId the block ID
   */
  public void scheduleRemoveFromWorker(long blockId) {
    mBlocks.remove(blockId);
    mToRemoveBlocks.add(blockId);
  }

  /**
   * Adds new worker lost storage paths.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * An exclusive lock is required.
   *
   * @param lostStorage the lost storage to add
   */
  public void addLostStorage(Map<String, StorageList> lostStorage) {
    for (Map.Entry<String, StorageList> entry : lostStorage.entrySet()) {
      List<String> paths = mUsage.mLostStorage.getOrDefault(entry.getKey(), new ArrayList<>());
      paths.addAll(entry.getValue().getStorageList());
      mUsage.mLostStorage.put(entry.getKey(), paths);
    }
  }

  /**
   * Gets the selected field information for this worker.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * A shared lock is required.
   *
   * @param fieldRange the client selected fields
   * @param isLiveWorker the worker is live or not
   * @return generated worker information
   */
  public WorkerInfo generateWorkerInfo(Set<WorkerInfoField> fieldRange, boolean isLiveWorker) {
    WorkerInfo info = new WorkerInfo();
    for (WorkerInfoField field : fieldRange) {
      switch (field) {
        case ADDRESS:
          info.setAddress(mMeta.mWorkerAddress);
          break;
        case BLOCK_COUNT:
          info.setBlockCount(getBlockCount());
          break;
        case WORKER_CAPACITY_BYTES:
          info.setCapacityBytes(mUsage.mCapacityBytes);
          break;
        case WORKER_CAPACITY_BYTES_ON_TIERS:
          info.setCapacityBytesOnTiers(mUsage.mTotalBytesOnTiers);
          break;
        case ID:
          info.setId(mMeta.mId);
          break;
        case LAST_CONTACT_SEC:
          info.setLastContactSec((int) ((CommonUtils.getCurrentMs()
              - mLastUpdatedTimeMs.get()) / Constants.SECOND_MS));
          break;
        case START_TIME_MS:
          info.setStartTimeMs(mMeta.mStartTimeMs);
          break;
        case STATE:
          if (isLiveWorker) {
            info.setState(LIVE_WORKER_STATE);
          } else {
            info.setState(LOST_WORKER_STATE);
          }
          break;
        case WORKER_USED_BYTES:
          info.setUsedBytes(mUsage.mUsedBytes);
          break;
        case WORKER_USED_BYTES_ON_TIERS:
          info.setUsedBytesOnTiers(mUsage.mUsedBytesOnTiers);
          break;
        default:
          LOG.warn("Unrecognized worker info field: " + field);
      }
    }
    return info;
  }

  /**
   * {@link StaticWorkerMeta} is thread safe so the value can be read without locking.
   *
   * @return the worker's address
   */
  public WorkerNetAddress getWorkerAddress() {
    return mMeta.mWorkerAddress;
  }

  /**
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * A shared lock is required.
   *
   * @return the available space of the worker in bytes
   */
  public long getAvailableBytes() {
    return mUsage.getAvailableBytes();
  }

  /**
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#BLOCKS} specified.
   * A shared lock is required.
   *
   * This returns a copy so the lock can be released when this method returns.
   *
   * @return ids of all blocks the worker contains
   */
  public Set<Long> getBlocks() {
    return new HashSet<>(mBlocks);
  }

  /**
   * Return the block count of this worker.
   *
   * @return the block count of this worker
   */
  public int getBlockCount() {
    return mBlocks.size();
  }

  /**
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * A shared lock is required.
   *
   * @return the capacity of the worker in bytes
   */
  public long getCapacityBytes() {
    return mUsage.mCapacityBytes;
  }

  /**
   * No locking required.
   *
   * @return the id of the worker
   */
  public long getId() {
    return mMeta.mId;
  }

  /**
   * No locking required.
   *
   * @return the last updated time of the worker in ms
   */
  public long getLastUpdatedTimeMs() {
    return mLastUpdatedTimeMs.get();
  }

  /**
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#BLOCKS} specified.
   * A shared lock is required.
   *
   * This returns a copy so the lock can be released after this method returns.
   *
   * @return ids of blocks the worker should remove
   */
  public Set<Long> getToRemoveBlocks() {
    return new HashSet<>(mToRemoveBlocks);
  }

  /**
   * @return used space of the worker in bytes
   */
  public long getUsedBytes() {
    return mUsage.mUsedBytes;
  }

  /**
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * A shared lock is required.
   *
   * @return the total bytes on each storage tier
   */
  public Map<String, Long> getTotalBytesOnTiers() {
    return mUsage.mTotalBytesOnTiers;
  }

  /**
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * A shared lock is required.
   *
   * @return the used bytes on each storage tier
   */
  public Map<String, Long> getUsedBytesOnTiers() {
    return mUsage.mUsedBytesOnTiers;
  }

  /**
   * No locking required.
   *
   * @return the start time in milliseconds
   */
  public long getStartTime() {
    return mMeta.mStartTimeMs;
  }

  /**
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#STATUS} specified.
   * A shared lock is required.
   *
   * @return whether the worker has been registered yet
   */
  public boolean isRegistered() {
    return mIsRegistered;
  }

  /**
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * A shared lock is required.
   *
   * @return the free bytes on each storage tier
   */
  public Map<String, Long> getFreeBytesOnTiers() {
    Map<String, Long> freeCapacityBytes = new HashMap<>();
    for (Map.Entry<String, Long> entry : mUsage.mTotalBytesOnTiers.entrySet()) {
      freeCapacityBytes.put(entry.getKey(),
          entry.getValue() - mUsage.mUsedBytesOnTiers.get(entry.getKey()));
    }
    return freeCapacityBytes;
  }

  /**
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * A shared lock is required.
   *
   * This returns a copy so the lock can be released after the map is returned.
   *
   * @return the map from tier alias to lost storage paths in this worker
   */
  public Map<String, List<String>> getLostStorage() {
    return new HashMap<>(mUsage.mLostStorage);
  }

  /**
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * A shared lock is required.
   *
   * @return true if this worker has lost storage, false otherwise
   */
  public boolean hasLostStorage() {
    return mUsage.mLostStorage.size() > 0;
  }

  @Override
  // TODO(jiacheng): Read lock on the conversion
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", mMeta.mId)
        .add("workerAddress", mMeta.mWorkerAddress)
        .add("capacityBytes", mUsage.mCapacityBytes)
        .add("usedBytes", mUsage.mUsedBytes)
        .add("lastUpdatedTimeMs", mLastUpdatedTimeMs.get())
        // We only show the number of blocks unless it is for DEBUG logs
        .add("blocks", LOG.isDebugEnabled() ? mBlocks : CommonUtils.summarizeCollection(mBlocks))
        .add("lostStorage", mUsage.mLostStorage).toString();
  }

  /**
   * Updates the last updated time of the worker in ms.
   * No locking is required.
   */
  public void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs.set(CommonUtils.getCurrentMs());
  }

  /**
   * Adds or removes a block from the to-be-removed blocks set of the worker.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#BLOCKS} specified.
   * An exclusive lock is required.
   *
   * @param add true if to add, to remove otherwise
   * @param blockId the id of the block to be added or removed
   */
  public void updateToRemovedBlock(boolean add, long blockId) {
    if (add) {
      if (mBlocks.contains(blockId)) {
        mToRemoveBlocks.add(blockId);
      }
    } else {
      mToRemoveBlocks.remove(blockId);
    }
  }

  /**
   * Sets the capacity of the worker in bytes.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * An exclusive lock is required.
   *
   * @param capacityBytesOnTiers used bytes on each storage tier
   */
  public void updateCapacityBytes(Map<String, Long> capacityBytesOnTiers) {
    long capacityBytes = 0;
    mUsage.mTotalBytesOnTiers = capacityBytesOnTiers;
    for (long t : mUsage.mTotalBytesOnTiers.values()) {
      capacityBytes += t;
    }
    mUsage.mCapacityBytes = capacityBytes;
  }

  /**
   * Sets the used space of the worker in bytes.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * An exclusive lock is required.
   *
   * @param usedBytesOnTiers used bytes on each storage tier
   */
  public void updateUsedBytes(Map<String, Long> usedBytesOnTiers) {
    long usedBytes = 0;
    mUsage.mUsedBytesOnTiers = new HashMap<>(usedBytesOnTiers);
    for (long t : mUsage.mUsedBytesOnTiers.values()) {
      usedBytes += t;
    }
    mUsage.mUsedBytes = usedBytes;
  }

  /**
   * Sets the used space of the worker in bytes.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * An exclusive lock is required.
   *
   * @param tierAlias alias of storage tier
   * @param usedBytesOnTier used bytes on certain storage tier
   */
  public void updateUsedBytes(String tierAlias, long usedBytesOnTier) {
    mUsage.mUsedBytes += usedBytesOnTier - mUsage.mUsedBytesOnTiers.get(tierAlias);
    mUsage.mUsedBytesOnTiers.put(tierAlias, usedBytesOnTier);
  }

  ReadWriteLock getLock(WorkerMetaLockSection lockType) {
    return mLockTypeToLock.get(lockType);
  }

  /**
   * Locks the corresponding locks on the metadata groups.
   * See the javadoc for this class for example usage.
   *
   * The locks will be acquired in order and later released in the opposite order.
   * The locks can either be shared or exclusive.
   * The isShared flag will apply to all the locks acquired here.
   *
   * This returns a {@link LockResource} which can be managed by a try-finally block.
   * See javadoc for {@link WorkerMetaLock} for more details about the internals.
   *
   * @param lockTypes the locks
   * @param isShared if false, the locking is exclusive
   * @return a {@link LockResource} of the {@link WorkerMetaLock}
   */
  public LockResource lockWorkerMeta(EnumSet<WorkerMetaLockSection> lockTypes, boolean isShared) {
    return new LockResource(new WorkerMetaLock(lockTypes, isShared, this));
  }

  /**
   * Returns the number of blocks that should be removed from the worker.
   *
   * @return the count
   */
  @VisibleForTesting
  public int getToRemoveBlockCount() {
    return mToRemoveBlocks.size();
  }

  /**
   * Updates the worker storage usage.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * An exclusive lock is required.
   *
   * @param globalStorageTierAssoc storage tier setup from configuration
   * @param storageTiers the storage tiers
   * @param totalBytesOnTiers the capacity of each tier
   * @param usedBytesOnTiers the current usage of each tier
   */
  public void updateUsage(StorageTierAssoc globalStorageTierAssoc, List<String> storageTiers,
      Map<String, Long> totalBytesOnTiers, Map<String, Long> usedBytesOnTiers) {
    mUsage.updateUsage(globalStorageTierAssoc, storageTiers, totalBytesOnTiers, usedBytesOnTiers);
  }

  /**
   * Marks all the blocks on the worker to be removed.
   * This is called at the beginning of a register stream, where we do not know what blocks
   * are no longer on the worker in {@link #mBlocks}.
   * First all blocks will be marked to-be-removed, then in the stream when we see a block list,
   * those blocks will be added to {@link #mBlocks} and removed from {@link #mToRemoveBlocks}.
   * In this way, at the end of the stream, {@link #mToRemoveBlocks} contains only the blocks
   * that no longer exist on the worker.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#BLOCKS} specified.
   * An exclusive lock is required.
   */
  public void markAllBlocksToRemove() {
    mToRemoveBlocks.addAll(mBlocks);
  }

  /**
   * Finds the read locks necessary for required worker information.
   *
   * @param fieldRange a set of {@link WorkerInfoField} that will be read
   * @return the required read locks
   */
  public static EnumSet<WorkerMetaLockSection> getLockTypesForInfo(
      Set<GetWorkerReportOptions.WorkerInfoField> fieldRange) {
    EnumSet<WorkerMetaLockSection> lockSections = EnumSet.noneOf(WorkerMetaLockSection.class);
    if (fieldRange.contains(GetWorkerReportOptions.WorkerInfoField.BLOCK_COUNT)) {
      lockSections.add(WorkerMetaLockSection.BLOCKS);
    }
    if (fieldRange.stream().anyMatch(USAGE_INFO_FIELDS::contains)) {
      lockSections.add(WorkerMetaLockSection.USAGE);
    }
    return lockSections;
  }
}
