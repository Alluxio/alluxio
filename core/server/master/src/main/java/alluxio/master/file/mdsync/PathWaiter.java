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

package alluxio.master.file.mdsync;

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
