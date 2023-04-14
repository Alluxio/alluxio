package alluxio.master.metastore.rocks;

/**
 * An object wrapper for RocksDB status. Two states are included.
 * The StopServing flag is an indicator that RocksDB will stop serving shortly.
 * This can be because the RocksDB will be closed, rewritten or wiped out.
 * This StopServing flag is used in:
 * 1. The shared lock will check this flag and give up the access early
 * 2. An ongoing r/w (e.g. an iterator) will check this flag during iteration
 *    and abort the iteration. So it will not block the RocksDB from shutting down.
 *
 * The version is needed because RocksBlockMetaStore and RocksInodeStore may clear and restart
 * the RocksDB. If the r/w enters after the restart, it should also abort because the RocksDB
 * may not have the data to operate on.
 */
public class VersionedRocksStoreStatus {
  public final boolean mStopServing;
  public final int mVersion;

  public VersionedRocksStoreStatus(boolean closed, int version) {
    mStopServing = closed;
    mVersion = version;
  }
}