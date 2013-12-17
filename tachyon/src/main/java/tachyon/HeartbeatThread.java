/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import org.apache.log4j.Logger;

/**
 * Thread class to execute a heartbeat periodically.
 */
public class HeartbeatThread extends Thread {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final String THREAD_NAME;
  private final HeartbeatExecutor EXECUTOR;
  private final long FIXED_EXECUTION_INTERVAL_MS;

  private boolean mIsShutdown = false;

  /**
   * @param threadName
   * @param hbExecutor
   * @param fixedExecutionIntervalMs Sleep time between different heartbeat.
   */
  public HeartbeatThread(String threadName, HeartbeatExecutor hbExecutor, 
      long fixedExecutionIntervalMs) {
    THREAD_NAME = threadName;
    EXECUTOR = hbExecutor;
    FIXED_EXECUTION_INTERVAL_MS = fixedExecutionIntervalMs;
  }

  public void run() {
    while (!mIsShutdown) {
      long lastMs = System.currentTimeMillis();
      EXECUTOR.heartbeat();
      try {
        long executionTimeMs = System.currentTimeMillis() - lastMs;
        if (executionTimeMs > FIXED_EXECUTION_INTERVAL_MS) {
          LOG.error(THREAD_NAME + " last execution took " + executionTimeMs + " ms. Longer than " +
              " the FIXED_EXECUTION_INTERVAL_MS "+ FIXED_EXECUTION_INTERVAL_MS);
        } else {
          Thread.sleep(FIXED_EXECUTION_INTERVAL_MS - executionTimeMs);
        }
      } catch (InterruptedException e) {
        LOG.info(e.getMessage(), e);
      }
    }
  }

  public void shutdown() {
    mIsShutdown = true;
  }
}
