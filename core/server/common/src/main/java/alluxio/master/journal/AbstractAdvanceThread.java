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

package alluxio.master.journal;

import alluxio.ProcessUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract helper for tracking and terminating a thread for journal advance task.
 */
public abstract class AbstractAdvanceThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAdvanceThread.class);

  @Override
  public void run() {
    try {
      runAdvance();
    } catch (Exception e) {
      ProcessUtils.fatalError(LOG, e, "Advance thread is failed.");
    }
  }

  /**
   * Cancels advancing gracefully.
   */
  public abstract void cancel();

  /**
   * Waits until advancing is finished (completed/cancelled).
   */
  public void waitTermination() {
    try {
      // Wait until thread terminates or timeout elapses.
      join(0);
    } catch (Exception e) {
      ProcessUtils.fatalError(LOG, e, "Advance task failed.");
    }
  }

  /**
   * Does the work of advancing.
   */
  protected abstract void runAdvance();
}
