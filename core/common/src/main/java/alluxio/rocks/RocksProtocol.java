package alluxio.rocks;

import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.resource.LockResource;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RocksProtocol {
  /**
   * When mClosing is true, stop serving all requests as the RocksDB is being closed.
   * This flag serves two purposes:
   * 1. Before an r/w operation, if this is observed then bail the operation early.
   * 2. During an r/w operation like iteration, if this is observed then abort the iteration.
   *
   * The version exists because we want to tell when the RocksDB is cleared on a failover.
   */
  protected final AtomicReference<VersionedRocksStoreStatus> mStatus;
  /**
   * The RWLock is used to guarantee thread safety between r/w and RocksDB closing/restarting.
   * When the RocksDB is closing/restarting, no concurrent r/w should be present.
   * Therefore, we acquire the write lock on closing/restarting, and acquire a read lock for
   * reading/writing the RocksDB.
   */
  protected final ReadWriteLock mDbStateLock = new ReentrantReadWriteLock();

  public RocksProtocol() {
    mStatus = new AtomicReference<>(new VersionedRocksStoreStatus(false, 0));
  }

  // TODO(jiacheng): double check what happens if max lock count error here
  public LockResource checkAndAcquireReadLock() {
    // Checking before locking to bail early
    VersionedRocksStoreStatus status = mStatus.get();
    if (status.mClosing) {
      throw new UnavailableRuntimeException(
          "RocksDB is closed. Master is failing over or shutting down.");
    }
    LockResource lock = new LockResource(mDbStateLock.readLock());
    /*
     * Counter-intuitively, check again after getting the lock because
     * we may get the read lock after the writer
     * The ref is different if the RocksDB is closed or restarted
     * If the RocksDB is restarted and cleared, we should abort even if it is serving
     */
    if (mStatus.get() != status) {
      lock.close();
      throw new UnavailableRuntimeException(
          "RocksDB is closed. Master is failing over or shutting down.");
    }
    return lock;
  }

  public LockResource lockForClosing() {
    mStatus.getAndUpdate((current) -> new VersionedRocksStoreStatus(true, current.mVersion));
    return new LockResource(mDbStateLock.writeLock());
  }

  public void abortIfClosing() {
    if (mStatus.get().mClosing) {
      throw new UnavailableRuntimeException(
          "RocksDB is closed. Master is failing over or shutting down.");
    }
  }

  public void updateVersionAndReopen() {
    mStatus.getAndUpdate((current) -> new VersionedRocksStoreStatus(false, current.mVersion + 1));
  }

  public static class VersionedRocksStoreStatus {
    public final boolean mClosing;
    public final int mVersion;

    public VersionedRocksStoreStatus(boolean closed, int version) {
      mClosing = closed;
      mVersion = version;
    }
  }
}
