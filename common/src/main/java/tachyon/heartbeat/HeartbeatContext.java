/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.heartbeat;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;

/**
 * This is a static class for storing and retrieving heartbeat related information.
 */
public final class HeartbeatContext {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static Map<String, Class<HeartbeatTimer>> sTimerClasses;

  // Names of different heartbeat timer classes.
  public static final Class<HeartbeatTimer> SCHEDULED_TIMER_CLASS;
  public static final Class<HeartbeatTimer> SLEEPING_TIMER_CLASS;

  // Names of different heartbeat executors.
  public static final String MASTER_CHECKPOINT_SCHEDULING = "Master Checkpoint Scheduling";
  public static final String MASTER_FILE_RECOMPUTATION = "Master File Recomputation";
  public static final String MASTER_LOST_WORKER_DETECTION = "Master Lost Worker Detection";
  public static final String MASTER_TTL_CHECK = "Master TTL Check";
  public static final String WORKER_LINEAGE_SYNC = "Worker Lineage Sync";
  public static final String WORKER_CLIENT = "Worker Client";

  static {
    try {
      SCHEDULED_TIMER_CLASS =
          (Class<HeartbeatTimer>) Class.forName("tachyon.heartbeat.ScheduledTimer");
      SLEEPING_TIMER_CLASS =
          (Class<HeartbeatTimer>) Class.forName("tachyon.heartbeat.SleepingTimer");
    } catch (Exception e) {
      throw new RuntimeException("requested class could not be loaded: " + e.getMessage());
    }
    sTimerClasses = new HashMap<String, Class<HeartbeatTimer>>();
    sTimerClasses.put(MASTER_CHECKPOINT_SCHEDULING, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(MASTER_FILE_RECOMPUTATION, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(MASTER_LOST_WORKER_DETECTION, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(MASTER_TTL_CHECK, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(WORKER_LINEAGE_SYNC, SLEEPING_TIMER_CLASS);
    sTimerClasses.put(WORKER_CLIENT, SLEEPING_TIMER_CLASS);
  }

  private HeartbeatContext() {} // to prevent initialization

  /**
   * @param name a name of a heartbeat executor thread
   * @return the timer class to use for the executor thread
   */
  public static synchronized  Class<HeartbeatTimer> getTimerClass(String name) {
    return sTimerClasses.get(name);
  }

  /**
   * @param name a name of a heartbeat executor thread
   * @param timerClass the timer class to use for the executor thread
   */
  public static synchronized void setTimerClass(String name, Class<HeartbeatTimer> timerClass) {
    sTimerClasses.put(name, timerClass);
  }
}
