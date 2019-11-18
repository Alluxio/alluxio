package alluxio.master.block.meta;

import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.resource.LockResource;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Source of truth for the master worker information that are not highly concurrent.
 *
 * Thread safety in this class is guaranteed by a read write lock.
 *
 * If one kind of worker information is highly concurrent,
 * it should be moved out of this class and controlled directly by {@link MasterWorkerInfo}.
 */
@ThreadSafe
public class StableWorkerInfo {
  /** Start time of the worker in ms. This is a read-only field. */
  private final long mStartTimeMs;
  /** Worker's address. This is a read-only field. */
  private final WorkerNetAddress mWorkerAddress;
  /** The id of the worker.This is a read-only field.*/
  private final long mId;

  /** Lock for stable worker info that has a small chance of being modified. */
  private final ReadWriteLock mLock = new ReentrantReadWriteLock();

  /** If true, the worker is considered registered. */
  @GuardedBy("mLock")
  private boolean mIsRegistered;
  /** Worker-specific mapping between storage tier alias and storage tier ordinal. */
  @GuardedBy("mLock")
  private StorageTierAssoc mStorageTierAssoc;

  /**
   * Creates a new instance of {@link StableWorkerInfo}.
   *
   * @param id the worker id to use
   * @param address the worker address to use
   */
  public StableWorkerInfo(long id, WorkerNetAddress address) {
    mWorkerAddress = Preconditions.checkNotNull(address, "address");
    mId = id;
    mStartTimeMs = System.currentTimeMillis();

    mIsRegistered = false;
    mStorageTierAssoc = null;
  }

  /**
   * @return the id of the worker
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the start time in milliseconds
   */
  public long getStartTime() {
    return mStartTimeMs;
  }

  /**
   * @return the worker's address
   */
  public WorkerNetAddress getWorkerAddress() {
    return mWorkerAddress;
  }

  /**
   * @return the storage tier mapping for the worker
   */
  public StorageTierAssoc getStorageTierAssoc() {
    try (LockResource r = new LockResource(mLock.readLock())) {
      return mStorageTierAssoc;
    }
  }

  /**
   * @return whether the worker has been registered yet
   */
  public boolean isRegistered() {
    try (LockResource r = new LockResource(mLock.readLock())) {
      return mIsRegistered;
    }
  }

  /**
   * Set the storage tier mapping for the worker.
   *
   * @param storageTierAliases list of storage tier aliases in order of their position in the
   *        hierarchy
   */
  void setStorageTierAssoc(List<String> storageTierAliases) {
    try (LockResource r = new LockResource(mLock.writeLock())) {
      mStorageTierAssoc = new WorkerStorageTierAssoc(storageTierAliases);
    }
  }

  /**
   * Set the register status.
   *
   * @param registered true if the worker is registered, false otherwise
   */
  void setRegistered(boolean registered) {
    try (LockResource r = new LockResource(mLock.writeLock())) {
      mIsRegistered = registered;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", mId).add("workerAddress", mWorkerAddress)
        .add("startTimeMs", mStartTimeMs).add("isRegistered", mIsRegistered)
        .add("storageTierAssoc", mStorageTierAssoc).toString();
  }
}
