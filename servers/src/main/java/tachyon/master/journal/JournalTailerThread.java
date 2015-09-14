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

package tachyon.master.journal;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.master.Master;
import tachyon.util.CommonUtils;

public final class JournalTailerThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  // TODO(gene): make the quiet period a configuration parameter.
  private static final int SHUTDOWN_QUIET_WAIT_TIME_MS = 5 * Constants.SECOND_MS;
  // TODO(gene): make this sleep time  a config parameter.
  private static final int JOURNAL_TAILER_SLEEP_TIME_MS = 1 * Constants.SECOND_MS;

  private final Master mMaster;
  private final Journal mJournal;
  /** This become true when this class is instructed to shutdown. */
  private volatile boolean mInitiateShutdown = false;

  /**
   * @param master the master to apply the journal entries to
   * @param journal the journal to tail
   */
  public JournalTailerThread(Master master, Journal journal) {
    mMaster = Preconditions.checkNotNull(master);
    mJournal = Preconditions.checkNotNull(journal);
  }

  /**
   * Initiate the shutdown of this tailer thread.
   */
  public void shutdown() {
    LOG.info(mMaster.getServiceName() + " Journal tailer shutdown has been initiated.");
    mInitiateShutdown = true;
  }

  /**
   * Initiate the shutdown of this tailer thread, and also wait for it to finish.
   */
  public void shutdownAndJoin() {
    shutdown();
    try {
      // Wait for the thread to finish.
      join();
    } catch (InterruptedException ie) {
      LOG.warn("stopping the journal tailer caused exception: " + ie.getMessage());
    }
  }

  @Override
  public void run() {
    LOG.info(mMaster.getServiceName() + " Journal tailer started.");
    // Continually loop loading the checkpoint file, and then loading all completed files. The loop
    // only repeats when the checkpoint file is updated after it was read.
    while (!mInitiateShutdown) {
      try {
        // The start time (ms) for the initiated shutdown.
        long waitForShutdownStart = -1;

        // Load the checkpoint file.
        LOG.info("Waiting to load the checkpoint file.");
        JournalTailer tailer = new JournalTailer(mMaster, mJournal);
        while (!tailer.checkpointExists()) {
          CommonUtils.sleepMs(LOG, JOURNAL_TAILER_SLEEP_TIME_MS);
          if (mInitiateShutdown) {
            LOG.info("Journal tailer is shutdown when waiting to load the checkpoint file.");
            return;
          }
        }
        LOG.info("Start loading the checkpoint file.");
        tailer.processJournalCheckpoint(true);
        LOG.info("Checkpoint file has been loaded.");

        // Continually process completed log files.
        while (tailer.isValid()) {
          if (tailer.processNextJournalLogFiles() > 0) {
            // Reset the shutdown timer.
            waitForShutdownStart = -1;
          } else {
            if (mInitiateShutdown) {
              if (waitForShutdownStart == -1) {
                waitForShutdownStart = CommonUtils.getCurrentMs();
              } else if ((CommonUtils.getCurrentMs()
                  - waitForShutdownStart) > SHUTDOWN_QUIET_WAIT_TIME_MS) {
                // There have been no new logs for the quiet period. Shutdown now.
                LOG.info(mMaster.getServiceName()
                    + " Journal tailer has been shutdown. No new logs for the quiet period.");
                return;
              }
            }
            LOG.info("The next complete log file does not exist yet. Sleeping and checking again.");
            CommonUtils.sleepMs(LOG, JOURNAL_TAILER_SLEEP_TIME_MS);
          }
        }
        LOG.info("The checkpoint is out of date. Will reload the checkpoint file.");
        CommonUtils.sleepMs(LOG, JOURNAL_TAILER_SLEEP_TIME_MS);
      } catch (IOException ioe) {
        // Log the error and continue the loop.
        LOG.error(ioe.getMessage());
      }
    }
    LOG.info(mMaster.getServiceName() + " Journal tailer has been shutdown.");
  }
}
