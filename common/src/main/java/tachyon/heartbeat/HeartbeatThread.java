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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.util.CommonUtils;

/**
 * Thread class to execute a heartbeat periodically. This thread is daemonic, so it will not prevent
 * the JVM from exiting.
 */
public final class HeartbeatThread implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final String mThreadName;
  private final HeartbeatExecutor mExecutor;
  private HeartbeatTimer mTimer;

  /**
   * @param threadName Identifies the heartbeat thread name.
   * @param executor Identifies the heartbeat thread executor; an instance of a class that
   *        implements the HeartbeatExecutor interface.
   * @param intervalMs Sleep time between different heartbeat.
   */
  public HeartbeatThread(String threadName, HeartbeatExecutor executor, long intervalMs) {
    mThreadName = threadName;
    mExecutor = Preconditions.checkNotNull(executor);
    Class<HeartbeatTimer> timerClass = HeartbeatContext.getTimerClass(threadName);
    try {
      mTimer =
          CommonUtils.createNewClassInstance(timerClass, new Class[] {String.class, long.class},
              new Object[] {threadName, intervalMs});
    } catch (Exception e) {
      String msg = "timer class could not be instantiated";
      LOG.error("{} : {} , {}", msg, threadName, e);
      mTimer = new SleepingTimer(threadName, intervalMs);
    }
  }

  @Override
  public void run() {
    // set the thread name
    Thread.currentThread().setName(mThreadName);
    try {
      while (!Thread.interrupted()) {
        mTimer.tick();
        mExecutor.heartbeat();
      }
    } catch (InterruptedException e) {
      // exit, reset interrupt
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOG.error("Uncaught exception in heartbeat executor, Heartbeat Thread shutting down", e);
    }
  }
}
