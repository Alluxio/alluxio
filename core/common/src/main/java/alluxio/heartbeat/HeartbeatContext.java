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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This is a static class for storing and retrieving heartbeat related information.
 */
@ThreadSafe
public final class HeartbeatContext {
  private static Map<String, Class<? extends HeartbeatTimer>> sTimerClasses;

  // Names of different heartbeat timer classes.
  public static final Class<? extends HeartbeatTimer> SCHEDULED_TIMER_CLASS = ScheduledTimer.class;
  public static final Class<? extends HeartbeatTimer> SLEEPING_TIMER_CLASS = SleepingTimer.class;

  // Names of different heartbeat executors.
  public static final String MASTER_BLOCK_INTEGRITY_CHECK = "Master Block Integrity Check";
  public static final String MASTER_CLUSTER_METRICS_UPDATER = "Master Cluster Metrics Updater";
  public static final String MASTER_CHECKPOINT_SCHEDULING = "Master Checkpoint Scheduling";
  public static final String MASTER_FILE_RECOMPUTATION = "Master File Recomputation";
  public static final String MASTER_LOG_CONFIG_REPORT_SCHEDULING
      = "Master Log Config Report Scheduling";
  public static final String MASTER_LOST_FILES_DETECTION = "Master Lost Files Detection";
  public static final String MASTER_LOST_MASTER_DETECTION = "Master Lost Master Detection";
  public static final String MASTER_LOST_WORKER_DETECTION = "Master Lost Worker Detection";
  public static final String MASTER_METRICS_SYNC = "Master Metrics Sync";
  public static final String MASTER_UFS_CLEANUP = "Master Ufs Cleanup";
  public static final String MASTER_TTL_CHECK = "Master TTL Check";
  public static final String META_MASTER_SYNC = "Meta Master Sync";
  public static final String WORKER_BLOCK_SYNC = "Worker Block Sync";
  public static final String WORKER_CLIENT = "Worker Client";
  public static final String WORKER_FILESYSTEM_MASTER_SYNC = "Worker FileSystemMaster Sync";
  public static final String WORKER_PIN_LIST_SYNC = "Worker Pin List Sync";
  public static final String WORKER_SPACE_RESERVER = "Worker Space Reserver";

  static {
    sTimerClasses = new HashMap<>();
    sTimerClasses.put(MASTER_BLOCK_INTEGRITY_CHECK, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(MASTER_CLUSTER_METRICS_UPDATER, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(MASTER_CHECKPOINT_SCHEDULING, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(MASTER_FILE_RECOMPUTATION, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(MASTER_LOG_CONFIG_REPORT_SCHEDULING, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(MASTER_LOST_FILES_DETECTION, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(MASTER_LOST_MASTER_DETECTION, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(MASTER_LOST_WORKER_DETECTION, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(MASTER_UFS_CLEANUP, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(MASTER_TTL_CHECK, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(META_MASTER_SYNC, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(WORKER_FILESYSTEM_MASTER_SYNC, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(WORKER_BLOCK_SYNC, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(WORKER_CLIENT, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(WORKER_PIN_LIST_SYNC, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(WORKER_SPACE_RESERVER, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(MASTER_METRICS_SYNC, SLEEPING_TIMER_CLASS);
  }

  private HeartbeatContext() {} // to prevent initialization

  /**
   * @return the mapping from executor thread names to timer classes
   */
  public static synchronized Map<String, Class<? extends HeartbeatTimer>> getTimerClasses() {
    return Collections.unmodifiableMap(sTimerClasses);
  }

  /**
   * @param name a name of a heartbeat executor thread
   * @return the timer class to use for the executor thread
   */
  public static synchronized Class<? extends HeartbeatTimer> getTimerClass(String name) {
    return sTimerClasses.get(name);
  }

  /**
   * Sets the timer class to use for the specified executor thread.
   *
   * This method should only be used by tests.
   *
   * @param name a name of a heartbeat executor thread
   * @param timerClass the timer class to use for the executor thread
   */
  @SuppressWarnings("unused")
  private static synchronized void setTimerClass(String name,
      Class<? extends HeartbeatTimer> timerClass) {
    if (timerClass == null) {
      sTimerClasses.remove(name);
    } else {
      sTimerClasses.put(name, timerClass);
    }
  }
}
