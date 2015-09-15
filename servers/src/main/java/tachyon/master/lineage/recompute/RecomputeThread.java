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

package tachyon.master.lineage.recompute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;

/**
 * Thread class to execute a recompute manager periodically. Thread is daemonic, so it will not
 * prevent the JVM from exiting.
 */
public final class RecomputeThread implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final String mThreadName;
  private final RecomputePlanner mPlanner;
  private final RecomputeExecutor mExecutor;
  private final long mFixedExecutionIntervalMs;

  /**
   * @param threadName identifies the checkpoint thread name.
   * @param planner recompute planner
   * @param executor recompute executor
   * @param fixedExecutionIntervalMs sleep time between checking.
   */
  public RecomputeThread(String threadName, RecomputePlanner planner, RecomputeExecutor executor,
      long fixedExecutionIntervalMs) {
    mThreadName = threadName;
    mPlanner = Preconditions.checkNotNull(planner);
    mExecutor = Preconditions.checkNotNull(executor);
    mFixedExecutionIntervalMs = fixedExecutionIntervalMs;
  }

  @Override
  public void run() {
    // set the thread name
    Thread.currentThread().setName(mThreadName);
    try {
      while (!Thread.interrupted()) {
        long lastMs = System.currentTimeMillis();

        RecomputePlan plan = mPlanner.plan();
        if (plan != null && plan.isEmpty()) {
          mExecutor.recompute(plan);
        }

        long executionTimeMs = System.currentTimeMillis() - lastMs;
        if (executionTimeMs > mFixedExecutionIntervalMs) {
          LOG.warn(mThreadName + " last checkpoint took " + executionTimeMs + " ms. Longer than "
              + " the mFixedExecutionIntervalMs " + mFixedExecutionIntervalMs);
        } else {
          Thread.sleep(mFixedExecutionIntervalMs - executionTimeMs);
        }
      }
    } catch (InterruptedException e) {
      // exit, reset interrupt
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOG.error("Uncaught exception in checkpoint executor, Checkpoint Thread shutting down", e);
    }

  }
}
