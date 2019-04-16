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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Base implementation for journal systems.
 */
@ThreadSafe
public abstract class AbstractJournalSystem implements JournalSystem {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractJournalSystem.class);

  private boolean mRunning = false;

  @Override
  public synchronized void start() throws InterruptedException, IOException {
    Preconditions.checkState(!mRunning, "Journal is already running");
    startInternal();
    mRunning = true;
  }

  @Override
  public synchronized void stop() throws InterruptedException, IOException {
    Preconditions.checkState(mRunning, "Journal is not running");
    mRunning = false;
    stopInternal();
  }

  /**
   * Starts the journal system.
   */
  protected abstract void startInternal() throws InterruptedException, IOException;

  /**
   * Stops the journal system.
   */
  protected abstract void stopInternal() throws InterruptedException, IOException;
}
