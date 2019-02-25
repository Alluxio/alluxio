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

package alluxio.master.journal.ufs;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.Journaled;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This thread continually replays the journal and applies it to the master, until the master
 * initiates the shutdown of the thread.
 * It periodically creates checkpoints. When the thread is stopped while it is writing checkpoint,
 * the checkpoint being written will be cancelled.
 */
@NotThreadSafe
public final class UfsJournalCheckpointThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalCheckpointThread.class);

  /** The master to apply the journal entries to. */
  private final Journaled mMaster;
  /** The journal. */
  private final UfsJournal mJournal;
  /** Make sure no new journal logs are found for this amount of time before shutting down. */
  private final long mShutdownQuietWaitTimeMs;
  /** If not journal log is found, sleep for this amount of time and check again. */
  private final int mJournalCheckpointSleepTimeMs;
  /** Writes a new checkpoint after processing this many journal entries. */
  private final long mCheckpointPeriodEntries;
  /** This becomes true when the master initiates the shutdown. */
  private volatile boolean mShutdownInitiated = false;

  /** True if this thread is no longer running. */
  private volatile boolean mStopped = false;

  /** Controls whether the thread will wait for a quiet period to elapse before terminating. */
  private volatile boolean mWaitQuietPeriod = true;

  /** The journal reader. */
  private JournalReader mJournalReader;

  /**
   * The next sequence number of the log journal entry to checkpoint.
   */
  private long mNextSequenceNumberToCheckpoint;

  /**
   * Creates a new instance of {@link UfsJournalCheckpointThread}.
   *
   * @param master the master to apply the journal entries to
   * @param journal the journal
   */
  public UfsJournalCheckpointThread(Journaled master, UfsJournal journal) {
    mMaster = Preconditions.checkNotNull(master, "master");
    mJournal = Preconditions.checkNotNull(journal, "journal");
    mShutdownQuietWaitTimeMs = journal.getQuietPeriodMs();
    mJournalCheckpointSleepTimeMs =
        (int) ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS);
    mJournalReader = new UfsJournalReader(mJournal, 0, false);
    mCheckpointPeriodEntries = ServerConfiguration.getLong(
        PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES);
  }

  /**
   * Initiates the shutdown of this checkpointer thread, and also waits for it to finish.
   *
   * @param waitQuietPeriod whether to wait for a quiet period to pass before terminating the thread
   */
  public void awaitTermination(boolean waitQuietPeriod) {
    LOG.info("{}: Journal checkpointer shutdown has been initiated.", mMaster.getName());
    mWaitQuietPeriod = waitQuietPeriod;
    mShutdownInitiated = true;
    // Actively interrupt to cancel slow checkpoints.
    interrupt();

    try {
      // Wait for the thread to finish.
      join();
      LOG.info("{}: Journal shutdown complete", mMaster.getName());
    } catch (InterruptedException e) {
      LOG.error("{}: journal checkpointer shutdown is interrupted.", mMaster.getName(), e);
      // Kills the master. This can happen in the following two scenarios:
      // 1. The user Ctrl-C the server.
      // 2. Zookeeper selects this master as standby before the master finishes the previous
      //    standby->leader transition. It is safer to crash the server because the behavior is
      //    undefined to have two journal checkpointer running concurrently.
      throw new RuntimeException(e);
    }
  }

  /**
   * This should only be called after {@link UfsJournalCheckpointThread#awaitTermination(boolean)}.
   *
   * @return the last edit log sequence number read plus 1
   */
  public long getNextSequenceNumber() {
    Preconditions.checkState(mStopped);
    return mJournalReader.getNextSequenceNumber();
  }

  @Override
  public void run() {
    try {
      runInternal();
    } catch (Throwable e) {
      LOG.error("{}: Failed to run journal checkpoint thread, crashing.", mMaster.getName(), e);
      throw e;
    }
  }

  private void runInternal() {
    // Keeps reading journal entries. If none is found, sleep for sometime. Periodically write
    // checkpoints if some conditions are met. When a shutdown signal is received, wait until
    // no new journal entries.

    LOG.info("{}: Journal checkpoint thread started.", mMaster.getName());
    // Set to true if it has waited for a quiet period. Reset if a valid journal entry is read.
    boolean quietPeriodWaited = false;
    while (true) {
      JournalEntry entry = null;
      try {
        switch (mJournalReader.advance()) {
          case CHECKPOINT:
            mMaster.restoreFromCheckpoint(mJournalReader.getCheckpoint());
            break;
          case LOG:
            entry = mJournalReader.read();
            mMaster.processJournalEntry(entry);
            if (quietPeriodWaited) {
              LOG.info("Quiet period interrupted by new journal entry");
              quietPeriodWaited = false;
            }
            break;
          default:
            break;
        }
      } catch (IOException e) {
        LOG.warn("{}: Failed to read or process the journal entry with error {}.",
            mMaster.getName(), e.getMessage());
        try {
          mJournalReader.close();
        } catch (IOException ee) {
          LOG.warn("{}: Failed to close the journal reader with error {}.", mMaster.getName(),
              ee.getMessage());
        }
        long nextSequenceNumber = mJournalReader.getNextSequenceNumber();

        mJournalReader = new UfsJournalReader(mJournal, nextSequenceNumber, false);
        quietPeriodWaited = false;
        continue;
      }

      // Sleep for a while if no entry is found.
      if (entry == null) {
        // Only try to checkpoint when it can keep up.
        maybeCheckpoint();
        if (mShutdownInitiated) {
          if (quietPeriodWaited || !mWaitQuietPeriod) {
            LOG.info("{}: Journal checkpoint thread has been shutdown. No new logs have been found "
                + "during the quiet period.", mMaster.getName());
            mStopped = true;

            if (mJournalReader != null) {
              try {
                mJournalReader.close();
              } catch (IOException e) {
                LOG.warn("{}: Failed to close the journal reader with error {}.", mMaster.getName(),
                    e.getMessage());
              }
            }
            return;
          }
          CommonUtils.sleepMs(LOG, mShutdownQuietWaitTimeMs);
          quietPeriodWaited = true;
        } else {
          CommonUtils.sleepMs(LOG, mJournalCheckpointSleepTimeMs);
        }
      }
      if (Thread.interrupted()) {
        if (!mShutdownInitiated) {
          LOG.info("Checkpoint thread interrupted, shutting down");
          return;
        }
      }
    }
  }

  /**
   * Creates a new checkpoint if necessary.
   */
  private void maybeCheckpoint() {
    if (mShutdownInitiated) {
      return;
    }
    long nextSequenceNumber = mJournalReader.getNextSequenceNumber();
    if (nextSequenceNumber - mNextSequenceNumberToCheckpoint < mCheckpointPeriodEntries) {
      return;
    }
    try {
      mNextSequenceNumberToCheckpoint = mJournal.getNextSequenceNumberToCheckpoint();
    } catch (IOException e) {
      LOG.warn("{}: Failed to get the next sequence number to checkpoint with error {}.",
          mMaster.getName(), e.getMessage());
      return;
    }
    if (nextSequenceNumber - mNextSequenceNumberToCheckpoint < mCheckpointPeriodEntries) {
      return;
    }

    writeCheckpoint(nextSequenceNumber);
  }

  private void writeCheckpoint(long nextSequenceNumber) {
    LOG.info("{}: Writing checkpoint [sequence number {}].", mMaster.getName(), nextSequenceNumber);
    try {
      try (UfsJournalCheckpointWriter journalWriter =
          mJournal.getCheckpointWriter(nextSequenceNumber)) {
        mMaster.writeToCheckpoint(journalWriter);
      } catch (InterruptedException e) {
        LOG.info("{}: Cancelled checkpoint [sequence number {}].", mMaster.getName(),
            nextSequenceNumber);
        Thread.currentThread().interrupt();
        return;
      }
      LOG.info("{}: Finished checkpoint [sequence number {}].", mMaster.getName(),
          nextSequenceNumber);
      mNextSequenceNumberToCheckpoint = nextSequenceNumber;
    } catch (IOException e) {
      LOG.warn("{}: Failed to checkpoint with error {}.", mMaster.getName(), e.getMessage());
    }
  }
}
