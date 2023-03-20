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

package alluxio.rocks;

import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.resource.LockResource;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class contains the necessary mechanism to ensure thread safety when accessing the RocksDB.
 */
public class RocksLocker {
  protected final AtomicReference<VersionedRocksStoreStatus> mStatus;
  protected final ReadWriteLock mDbStateLock = new ReentrantReadWriteLock();

  public RocksLocker() {
    mStatus = new AtomicReference<>(new VersionedRocksStoreStatus(false, 0));
  }

  /**
   * Before any r/w operation on the RocksDB, acquire a shared lock with this method.
   * The shared lock guarantees the RocksDB will not be restarted/cleared during the
   * r/w access.
   */
  public LockResource checkAndAcquireReadLock() {
    /*
     * Checking before locking to bail early, this is for speed rather than correctness.
     */
    VersionedRocksStoreStatus status = mStatus.get();
    if (status.mClosing) {
      throw new UnavailableRuntimeException(
          "RocksDB is closed. Master is failing over or shutting down.");
    }
    LockResource lock = new LockResource(mDbStateLock.readLock());
    /*
     * Counter-intuitively, check again after getting the lock because
     * we may get the read lock after the writer.
     * The ref is different if the RocksDB is closed or restarted.
     * If the RocksDB is restarted(cleared), we should abort even if it is serving.
     */
    if (mStatus.get() != status) {
      lock.close();
      throw new UnavailableRuntimeException(
          "RocksDB is closed. Master is failing over or shutting down.");
    }
    return lock;
  }

  /**
   * Before the process shuts down, acquire an exclusive lock on the RocksDB before closing.
   * Note this lock only exists on the Alluxio side. A CLOSING flag will be set so all
   * existing readers/writers will abort asap.
   * The exclusive lock ensures there are no existing concurrent r/w operations, so it is safe to
   * close the RocksDB and recycle all relevant resources.
   *
   * The CLOSING status will NOT be reset, because the process will shut down soon.
   */
  public LockResource lockForClosing() {
    mStatus.getAndUpdate((current) -> new VersionedRocksStoreStatus(true, current.mVersion));
    return new LockResource(mDbStateLock.writeLock());
  }

  /**
   * Before the process shuts down, acquire an exclusive lock on the RocksDB before closing.
   * Note this lock only exists on the Alluxio side. A CLOSING flag will be set so all
   * existing readers/writers will abort asap.
   * The exclusive lock ensures there are no existing concurrent r/w operations, so it is safe to
   * close the RocksDB and recycle all relevant resources.
   *
   * The CLOSING status will be reset and the version will be updated, so if a later r/w operation
   * gets the shared lock, it is able to tell the RocksDB has been cleared.
   * See {@link #checkAndAcquireReadLock} for how this affects the shared lock logic.
   */
  public LockResource lockForClearing() {
    mStatus.getAndUpdate((current) -> new VersionedRocksStoreStatus(true, current.mVersion));
    return new LockResource(mDbStateLock.writeLock(), true, false, () -> {
      mStatus.getAndUpdate((current) -> new VersionedRocksStoreStatus(false, current.mVersion + 1));
    });
  }

  /**
   * Used by ongoing r/w operations to check if the operation needs to abort and yield
   * to the RocksDB shutdown.
   */
  public void abortIfClosing() {
    if (mStatus.get().mClosing) {
      throw new UnavailableRuntimeException(
          "RocksDB is closed. Master is failing over or shutting down.");
    }
  }

  /**
   * An object wrapper for RocksDB status. Two states are included.
   * The CLOSING flag is an indicator that RocksDB will be shut down shortly.
   * This CLOSING flag is used in:
   * 1. The shared lock will check this flag and give up the access early
   * 2. An ongoing r/w (e.g. an iterator) will check this flag during iteration
   *    and abort the iteration. So it will not block the RocksDB from shutting down.
   *
   * The version is needed because RocksBlockMetaStore and RocksInodeStore may clear and restart
   * the RocksDB. If the r/w enters after the restart, it should also abort because the RocksDB
   * may not have the data to operate on.
   */
  public static class VersionedRocksStoreStatus {
    public final boolean mClosing;
    public final int mVersion;

    public VersionedRocksStoreStatus(boolean closed, int version) {
      mClosing = closed;
      mVersion = version;
    }
  }
}
