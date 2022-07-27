package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.file.options.DescendantType;

/**
 * Interface for sync.
 */
public interface SyncPathCache {
  /**
   * notify sync happened.
   * @param path
   * @param descendantType
   */
  void notifySyncedPath(AlluxioURI path, DescendantType descendantType);

  /**
   * Called instead of notifySyncedPath in case of failure.
   * @param path
   */
  void failedSyncPath(AlluxioURI path);

  /**
   * check if sync should happen.
   * @param path
   * @param intervalMs
   * @param descendantType
   * @return true if should sync
   */
  boolean shouldSyncPath(AlluxioURI path, long intervalMs, DescendantType descendantType);

  /**
   * called when starting a sync.
   * @param path
   */
  void startSync(AlluxioURI path);
}
