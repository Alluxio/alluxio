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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.InvalidJournalEntryException;
import alluxio.master.Master;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This thread continually tails the journal and applies it to the master, until the master
 * initiates the shutdown of the thread.
 */
@NotThreadSafe
public final class JournalTailerThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(JournalTailerThread.class);

  /** The master to apply the journal entries to. */
  private final Master mMaster;
  /** The journal to tail. */
  private final Journal mJournal;
  private final int mShutdownQuietWaitTimeMs;
  private final int mJournalTailerSleepTimeMs;
  /** This becomes true when the master initiates the shutdown. */
  private volatile boolean mInitiateShutdown = false;

  private JournalReader mJournalReader;

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
    mShutdownQuietWaitTimeMs =
        Configuration.getInt(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS);
    mJournalTailerSleepTimeMs =
        Configuration.getInt(PropertyKey.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS);
    mJournalReader = mJournal.getReader();
  }

  /**
   * Initiates the shutdown of this tailer thread, and also waits for it to finish.
   */
  public void shutdownAndJoin() {
    LOG.info("{}: Journal tailer shutdown has been initiated.", mMaster.getName());
    mInitiateShutdown = true;

    try {
      // Wait for the thread to finish.
      join();
    } catch (InterruptedException e) {
      LOG.error("{}: journal tailer shutdown is interrupted.", mMaster.getName(), e);
      // Kills the master. This can happen in the following two scenarios:
      // 1. The user Ctrl-C the server.
      // 2. Zookeeper selects this master as standby before the master finishes the previous
      //    standby->leader transition. It is safer to crash the server because the behavior is
      //    undefined to have two journal tailer running concurrently.
      throw new RuntimeException(e);
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


  private boolean shouldCheckpoint() {
    return false;
  }

  void maybeCheckpoint() {
    if (!shouldCheckpoint()) {
      return;
    }
    Iterator<alluxio.proto.journal.Journal.JournalEntry> it = mMaster.iterator();
    JournalWriter journalWriter = null;
    IOException exception = null;
    try {
      journalWriter = mJournal.getWriter(JournalWriterCreateOptions.defaults().setPrimary(false));
      while (it.hasNext() && !mInitiateShutdown) {
        journalWriter.write(it.next());
      }
    } catch (IOException e) {
      LOG.warn("Failed to checkpoint with error {}.", e.getMessage());
      exception = e;
    }

    if (it.hasNext() || mInitiateShutdown || exception != null) {
      if (journalWriter != null) {
        try {
          journalWriter.cancel();
        } catch (IOException e) {
          LOG.warn("Failed to cancel the checkpoint with error {}.", e.getMessage());
        }
      }
    }
  }

  @Override
  public void run() {
    // 1. Reads the journal and replays the journal entries.
    // 2. Checkpoint when some condition is met.
    // NOTE: If any errors appears in the above process, start from scratch.

    LOG.info("{}: Journal tailer started.", mMaster.getName());
    // Continually loop loading the checkpoint file, and then loading all completed files. The loop
    // only repeats when the checkpoint file is updated after it was read.
    while (!mInitiateShutdown) {
      try {
        // The start time (ms) for the initiated shutdown.
        long waitForShutdownStart = -1;

        LOG.info("{}: Waiting to load the checkpoint file.", mMaster.getName());
        alluxio.proto.journal.Journal.JournalEntry entry = null;

        while (entry == null) {
          entry = mJournalReader.read();
          LOG.info("{}: No journal entry found. sleeping for {}ms.", mMaster.getName(),
              mJournalTailerSleepTimeMs);
          CommonUtils.sleepMs(LOG, mJournalTailerSleepTimeMs);
          if (mInitiateShutdown) {
            LOG.info("Journal tailer has been shutdown while waiting to load the checkpoint file.");
            mStopped = true;
            return;
          }
        }
        mMaster.processJournalEntry(entry);
        maybeCheckpoint();

        // Continually process completed log files.
        while (mJournalTailer.isValid()) {
          if (mJournalTailer.processNextJournalLogs() > 0) {
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
                    + "Sleeping {}ms and checking again.", mMaster.getName(),
                mJournalTailerSleepTimeMs);
            CommonUtils.sleepMs(LOG, mJournalTailerSleepTimeMs);
          }
        }
        LOG.info("{}: The checkpoint is out of date. Will reload the checkpoint file.",
            mMaster.getName());
        CommonUtils.sleepMs(LOG, mJournalTailerSleepTimeMs);
      } catch (IOException e) {
        // Log the error and continue the loop.
        LOG.error("Error in journal tailer thread", e);
      } catch (InvalidJournalEntryException e) {

      }
    }
    LOG.info("{}: Journal tailer has been shutdown.", mMaster.getName());
    mStopped = true;
  }
}
