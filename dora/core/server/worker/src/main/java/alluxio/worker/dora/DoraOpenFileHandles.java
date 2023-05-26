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
public class DoraOpenFileHandles extends Thread {
  private final Map<String, OpenFileHandle> mOpenFileHandles;
  private Boolean mStop;

  DoraOpenFileHandles() {
    mOpenFileHandles = new HashMap<>();
    mStop = Boolean.FALSE;
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
        // ignored.
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
    mStop = Boolean.TRUE;
    interrupt();
  }
}
