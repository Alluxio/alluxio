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

package alluxio.worker.dora;

import alluxio.Constants;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A collection of open file handles in a dora worker.
 *
 * It is also a thread, and will run periodic checking of stale open handles.
 */
public class DoraOpenFileHandleContainer extends Thread {
  private final Map<String, OpenFileHandle> mOpenFileHandles;
  private boolean mStop;

  DoraOpenFileHandleContainer() {
    mOpenFileHandles = new HashMap<>();
    mStop = false;
  }

  @Override
  public void run() {
    while (!mStop) {
      try {
        sleep(Constants.MINUTE_MS);
        // Iterate the mOpenFileHandles if some handles are stale (not active for a long time).
        Set<String> keys = mOpenFileHandles.keySet();
        for (String key : keys) {
          OpenFileHandle handle = mOpenFileHandles.get(key);
          if (System.currentTimeMillis() - handle.getLastAccessTimeMs() >= Constants.HOUR) {
            mOpenFileHandles.remove(key);
            handle.close();
          }
        }
      } catch (InterruptedException e) {
        // Ignored. If this is interrupted by shutdown(), we will stop.
      } catch (Exception e) {
        // Ignored. The thread will continue.
      }
    }
    // Now, it is stopped. Let's remove all handles.
    // Add code here to close all handles.
  }

  /**
   * Add a open file handle into this collection.
   * @param key
   * @param handle
   * @return true if succeeded, otherwise false is returned
   */
  public boolean add(String key, OpenFileHandle handle) {
    OpenFileHandle old = mOpenFileHandles.putIfAbsent(key, handle);
    return old == null;
  }

  /**
   * Wakeup the current thread and ask it to stop.
   */
  public void shutdown() {
    mStop = true;
    // This will wake up the current thread if it is in sleep().
    interrupt();
  }
}
