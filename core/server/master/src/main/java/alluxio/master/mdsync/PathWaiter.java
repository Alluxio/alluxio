package alluxio.master.mdsync;

import alluxio.AlluxioURI;

interface PathWaiter {

  /**
   * The calling thread will be blocked until the given path has been synced.
   * @param path the path to sync
   * @return true if the sync on the path was successful, false otherwise
   */
  boolean waitForSync(AlluxioURI path);

  /**
   * Called on each batch of results that has completed processing.
   * @param completed the completed results
   */
  void nextCompleted(SyncProcessResult completed);
}
