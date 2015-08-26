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

package tachyon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread class to execute a heartbeat periodically. This Thread is daemonic, so it will not prevent
 * the JVM from exiting.
 */
public final class HeartbeatThread implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final String mThreadName;
  private final HeartbeatExecutor mExecutor;
  private final long mFixedExecutionIntervalMs;

  /**
   * @param threadName Identifies the heartbeat thread name.
   * @param hbExecutor Identifies the heartbeat thread executor; an
   *        instance of a class that implements the HeartbeatExecutor
   *        interface.
   * @param fixedExecutionIntervalMs Sleep time between different heartbeat.
   */
  public HeartbeatThread(String threadName, HeartbeatExecutor hbExecutor,
      long fixedExecutionIntervalMs) {
    mThreadName = threadName;
    mExecutor = hbExecutor;
    mFixedExecutionIntervalMs = fixedExecutionIntervalMs;
  }

  @Override
  public void run() {
    // set the thread name
    Thread.currentThread().setName(mThreadName);
    try {
      while (!Thread.interrupted()) {
        long lastMs = System.currentTimeMillis();
        mExecutor.heartbeat();
        long executionTimeMs = System.currentTimeMillis() - lastMs;
        if (executionTimeMs > mFixedExecutionIntervalMs) {
          LOG.warn(mThreadName + " last execution took " + executionTimeMs + " ms. Longer than "
              + " the mFixedExecutionIntervalMs " + mFixedExecutionIntervalMs);
        } else {
          Thread.sleep(mFixedExecutionIntervalMs - executionTimeMs);
        }
      }
    } catch (InterruptedException e) {
      // exit, reset interrupt
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOG.error("Uncaught exception in heartbeat executor, Heartbeat Thread shutting down", e);
    }
  }
}
