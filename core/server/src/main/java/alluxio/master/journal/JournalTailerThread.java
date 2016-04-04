/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.journal;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.master.Master;
import alluxio.master.MasterContext;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This thread continually tails the journal and applies it to the master, until the master
 * initiates the shutdown of the thread.
 */
@NotThreadSafe
public final class JournalTailerThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The master to apply the journal entries to. */
  private final Master mMaster;
  /** The journal to tail. */
  private final Journal mJournal;
  private final int mShutdownQuietWaitTimeMs;
  private final int mJournalTailerSleepTimeMs;
  /** This becomes true when the master initiates the shutdown. */
  private volatile boolean mInitiateShutdown = false;

  /** The {@link JournalTailer} that this thread uses to continually tail the journal. */
  private JournalTailer mJournalTailer = null;
  /** True if this thread is no longer running. */
  private boolean mStopped = false;

  /**
   * Creates a new instance of {@link JournalTailerThread}.
   *
   * @param master the master to apply the journal entries to
   * @param journal the journal to tail
   */
  public JournalTailerThread(Master master, Journal journal) {
    mMaster = Preconditions.checkNotNull(master);
    mJournal = Preconditions.checkNotNull(journal);
    Configuration conf = MasterContext.getConf();
    mShutdownQuietWaitTimeMs = conf.getInt(
        Constants.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS);
    mJournalTailerSleepTimeMs = conf.getInt(Constants.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS);
  }

  /**
   * Initiates the shutdown of this tailer thread.
   */
  public void shutdown() {
    LOG.info("{}: Journal tailer shutdown has been initiated.", mMaster.getName());
    mInitiateShutdown = true;
  }

  /**
   * Initiates the shutdown of this tailer thread, and also waits for it to finish.
   */
  public void shutdownAndJoin() {
    shutdown();
    try {
      // Wait for the thread to finish.
      join();
    } catch (InterruptedException e) {
      LOG.warn("{}: stopping the journal tailer caused an exception", mMaster.getName(), e);
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
    LOG.info("{}: Journal tailer started.", mMaster.getName());
    // Continually loop loading the checkpoint file, and then loading all completed files. The loop
    // only repeats when the checkpoint file is updated after it was read.
    while (!mInitiateShutdown) {
      try {
        // The start time (ms) for the initiated shutdown.
        long waitForShutdownStart = -1;

        // Load the checkpoint file.
        LOG.info("{}: Waiting to load the checkpoint file.", mMaster.getName());
        mJournalTailer = new JournalTailer(mMaster, mJournal);
        while (!mJournalTailer.checkpointExists()) {
          CommonUtils.sleepMs(LOG, mJournalTailerSleepTimeMs);
          if (mInitiateShutdown) {
            LOG.info("Journal tailer has been shutdown while waiting to load the checkpoint file.");
            mStopped = true;
            return;
          }
        }
        LOG.info("{}: Start loading the checkpoint file.", mMaster.getName());
        mJournalTailer.processJournalCheckpoint(true);
        LOG.info("{}: Checkpoint file has been loaded.", mMaster.getName());

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
                LOG.info("{}: Journal tailer has been shutdown. No new logs for the quiet period.",
                    mMaster.getName());
                mStopped = true;
                return;
              }
            }
            LOG.debug("{}: The next complete log file does not exist yet. "
                + "Sleeping and checking again.", mMaster.getName());
            CommonUtils.sleepMs(LOG, mJournalTailerSleepTimeMs);
          }
        }
        LOG.info("{}: The checkpoint is out of date. Will reload the checkpoint file.",
            mMaster.getName());
        CommonUtils.sleepMs(LOG, mJournalTailerSleepTimeMs);
      } catch (IOException e) {
        // Log the error and continue the loop.
        LOG.error("Error in journal tailer thread", e);
      }
    }
    LOG.info("{}: Journal tailer has been shutdown.", mMaster.getName());
    mStopped = true;
  }
}
