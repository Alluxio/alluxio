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

package alluxio.heartbeat;

import alluxio.wire.HeartbeatThreadInfo;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Manager for heartbeat thread.
 */
public class HeartbeatThreadManager {
  private static final Map<String, HeartbeatThread> HEARTBEAT_THREAD_MAP
      = new ConcurrentHashMap<>();
  private static final Map<Object, List<HeartbeatThread>> HEARTBEAT_THREAD_INDEX_MAP
      = new ConcurrentHashMap<>();

  /**
   * Add a heartbeat thread.
   *
   * @param key the name of heartbeat thread
   * @param heartbeatThread the heartbeat thread
   */
  public static synchronized void register(Object key, HeartbeatThread heartbeatThread) {
    List<HeartbeatThread> list = HEARTBEAT_THREAD_INDEX_MAP.get(key);
    if (list == null) {
      list = new LinkedList<>();
      HEARTBEAT_THREAD_INDEX_MAP.put(key, list);
    }
    HEARTBEAT_THREAD_MAP.put(heartbeatThread.getThreadName(), heartbeatThread);
  }

  /**
   * Remove the heartbeat threads related to the given key.
   * @param key the key of heartbeat thread
   */
  public static synchronized void unregister(Object key) {
    List<HeartbeatThread> heartbeatThreads = HEARTBEAT_THREAD_INDEX_MAP.remove(key);
    if (heartbeatThreads != null) {
      for (HeartbeatThread heartbeatThread : heartbeatThreads) {
        HEARTBEAT_THREAD_MAP.remove(heartbeatThread.getThreadName());
      }
    }
  }

  /**
   * Submits a heartbeat thread for execution and returns a Future representing that task.
   * Meanwhile, register the heartbeat thread to HeartbeatThreadManager to monitor its status.
   * @param executorService the executor service for executing heartbeat threads
   * @param heartbeatThread the heartbeat thread
   * @return a Future representing pending completion of the task
   */
  public static Future<?> submit(ExecutorService executorService,
      HeartbeatThread heartbeatThread) {
    HeartbeatThreadManager.register(executorService, heartbeatThread);
    return executorService.submit(heartbeatThread);
  }

  /**
   * Gets heartbeat threads info.
   * @return the heartbeat threads info
   */
  public static synchronized Map<String, HeartbeatThreadInfo> getHeartbeatThreads() {
    SortedMap<String, HeartbeatThreadInfo> heartbeatThreads = new TreeMap<>();
    for (HeartbeatThread heartbeatThread : HEARTBEAT_THREAD_MAP.values()) {
      heartbeatThreads.put(heartbeatThread.getThreadName(),
          heartbeatThread.toHeartbeatThreadInfo());
    }
    return heartbeatThreads;
  }
}
