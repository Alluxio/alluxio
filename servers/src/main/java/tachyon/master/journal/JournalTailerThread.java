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
import tachyon.conf.TachyonConf;
import tachyon.master.Master;
import tachyon.util.CommonUtils;

public final class JournalTailerThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Master mMaster;
  private final Journal mJournal;
  private final int mShutdownQuietWaitTimeMs;
  private final int mJournalTailerSleepTimeMs;
  /** This become true when this class is instructed to shutdown. */
  private volatile boolean mInitiateShutdown = false;

  /** The {@link JournalTailer} that this thread uses to continually tail the journal. */
  private JournalTailer mJournalTailer = null;
  /** True if this thread is no longer running. */
  private boolean mStopped = false;

  /**
   * @param master the master to apply the journal entries to
   * @param journal the journal to tail
   * @param conf the TachyonConf to get configurable parameters from
   */
  public JournalTailerThread(Master master, Journal journal, TachyonConf conf) {
    mMaster = Preconditions.checkNotNull(master);
    mJournal = Preconditions.checkNotNull(journal);
    mShutdownQuietWaitTimeMs = conf.getInt(
        Constants.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS);
    mJournalTailerSleepTimeMs = conf.getInt(Constants.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS);
  }

  /**
   * Initiate the shutdown of this tailer thread.
   */
  public void shutdown() {
    LOG.info(mMaster.getServiceName() + ": Journal tailer shutdown has been initiated.");
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
      LOG.warn(mMaster.getServiceName() + ": stopping the journal tailer caused exception: "
          + ie.getMessage());
    }
  }

  /**
   * @return the {@link JournalTailer} that this thread last used to tail the journal. This will
   *         only return the {@link JournalTailer} if this thread is no longer running, to prevent
   *         concurrent access to the {@link JournalTailer}. Returns null if this thread has not yet
   *         used a {@link JournalTailer}, or if this thread is still running.
   */
  public JournalTailer getLatestJournalTailer() {
    if (mStopped) {
      return mJournalTailer;
    }
    return null;
  }

  @Override
  public void run() {
    LOG.info(mMaster.getServiceName() + ": Journal tailer started.");
    // Continually loop loading the checkpoint file, and then loading all completed files. The loop
    // only repeats when the checkpoint file is updated after it was read.
    while (!mInitiateShutdown) {
      try {
        // The start time (ms) for the initiated shutdown.
        long waitForShutdownStart = -1;

        // Load the checkpoint file.
        LOG.info(mMaster.getServiceName() + ": Waiting to load the checkpoint file.");
        mJournalTailer = new JournalTailer(mMaster, mJournal);
        while (!mJournalTailer.checkpointExists()) {
          CommonUtils.sleepMs(LOG, mJournalTailerSleepTimeMs);
          if (mInitiateShutdown) {
            LOG.info("Journal tailer has been shutdown while waiting to load the checkpoint file.");
            mStopped = true;
            return;
          }
        }
        LOG.info(mMaster.getServiceName() + ": Start loading the checkpoint file.");
        mJournalTailer.processJournalCheckpoint(true);
        LOG.info(mMaster.getServiceName() + ": Checkpoint file has been loaded.");

        // Continually process completed log files.
        while (mJournalTailer.isValid()) {
          if (mJournalTailer.processNextJournalLogFiles() > 0) {
            // Reset the shutdown timer.
            waitForShutdownStart = -1;
          } else {
            if (mInitiateShutdown) {
              if (waitForShutdownStart == -1) {
                waitForShutdownStart = CommonUtils.getCurrentMs();
              } else if ((CommonUtils.getCurrentMs()
                  - waitForShutdownStart) > mShutdownQuietWaitTimeMs) {
                // There have been no new logs for the quiet period. Shutdown now.
                LOG.info(mMaster.getServiceName()
                    + ": Journal tailer has been shutdown. No new logs for the quiet period.");
                mStopped = true;
                return;
              }
            }
            LOG.debug(mMaster.getServiceName()
                + ": The next complete log file does not exist yet. Sleeping and checking again.");
            CommonUtils.sleepMs(LOG, mJournalTailerSleepTimeMs);
          }
        }
        LOG.info(mMaster.getServiceName()
            + ": The checkpoint is out of date. Will reload the checkpoint file.");
        CommonUtils.sleepMs(LOG, mJournalTailerSleepTimeMs);
      } catch (IOException ioe) {
        // Log the error and continue the loop.
        LOG.error(ioe.getMessage());
      }
    }
    LOG.info(mMaster.getServiceName() + ": Journal tailer has been shutdown.");
    mStopped = true;
  }
}
