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
import alluxio.master.journal.options.JournalReaderCreateOptions;
import alluxio.master.journal.options.JournalWriterCreateOptions;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This thread continually replays the journal and applies it to the master, until the master
 * initiates the shutdown of the thread.
 * It periodically creates checkpoints. When the thread is stopped while it is writing checkpoint,
 * the checkpoint being written will be cancelled.
 */
@NotThreadSafe
public final class JournalCheckpointThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(JournalCheckpointThread.class);

  /** The master to apply the journal entries to. */
  private final Master mMaster;
  /** The journal. */
  private final Journal mJournal;
  /** Make sure no new journal logs are found for this amount of time before shutting down. */
  private final int mShutdownQuietWaitTimeMs;
  /** If not journal log is found, sleep for this amount of time and check again. */
  private final int mJournalCheckpointSleepTimeMs;
  /** This becomes true when the master initiates the shutdown. */
  private volatile boolean mInitiateShutdown = false;

  /** True if this thread is no longer running. */
  private volatile boolean mStopped = false;

  /** The journal reader. */
  private JournalReader mJournalReader;

  /**
   * Creates a new instance of {@link JournalCheckpointThread}.
   *
   * @param master the master to apply the journal entries to
   * @param journal the journal to tail
   */
  public JournalCheckpointThread(Master master, Journal journal) {
    mMaster = Preconditions.checkNotNull(master);
    mJournal = Preconditions.checkNotNull(journal);
    mShutdownQuietWaitTimeMs =
        Configuration.getInt(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS);
    mJournalCheckpointSleepTimeMs =
        Configuration.getInt(PropertyKey.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS);
    mJournalReader = mJournal.getReader(JournalReaderCreateOptions.defaults().setPrimary(false));
  }

  /**
   * Initiates the shutdown of this checkpointer thread, and also waits for it to finish.
   */
  public void shutdownAndJoin() {
    LOG.info("{}: Journal checkpointer shutdown has been initiated.", mMaster.getName());
    mInitiateShutdown = true;

    try {
      // Wait for the thread to finish.
      join();
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
   * @return the last edit log sequence number read plus 1
   */
  public long getNextSequenceNumber() {
    Preconditions.checkState(mStopped);
    return mJournalReader.getNextSequenceNumber();
  }

  @Override
  public void run() {
    // Keeps reading journal entries. If none is found, sleep for sometime. Periodically write
    // checkpoints if some conditions are met. The a shutdown signal is receivied, wait until
    // no new journal entries.

    LOG.info("{}: Journal checkpointer started.", mMaster.getName());
    alluxio.proto.journal.Journal.JournalEntry entry;
    while (true) {
      // The start time (ms) for the initiated shutdown.
      try {
        entry = mJournalReader.read();
        if (entry != null) {
          mMaster.processJournalEntry(entry);
        }
      } catch (IOException | InvalidJournalEntryException e) {
        LOG.warn("{}: Failed to process the journal entry with error {}.", mMaster.getName(),
            e.getMessage());
        try {
          mJournalReader.close();
        } catch (IOException ee) {
          LOG.warn("{}: Failed to close the journal reader with error {}.", mMaster.getName(),
              ee.getMessage());
        }
        mJournalReader =
            mJournal.getReader(JournalReaderCreateOptions.defaults().setPrimary(false));
        continue;
      }

      maybeCheckpoint();

      // Sleep for a while if no entry is found.
      if (entry == null) {
        if (mInitiateShutdown) {
          CommonUtils.sleepMs(LOG, mShutdownQuietWaitTimeMs);
          LOG.info(
              "{}: Journal checkpointer has been shutdown. No new logs have been found during the "
                  + "quiet period.", mMaster.getName());
          mStopped = true;
          return;
        }
        CommonUtils.sleepMs(LOG, mJournalCheckpointSleepTimeMs);
      }
    }
  }

  /**
   * Creates a new checkpoint if necessary.
   */
  private void maybeCheckpoint() {
    if (mInitiateShutdown) {
      return;
    }
    try {
      if (!mJournalReader.shouldCheckpoint()) {
        return;
      }
    } catch (IOException e) {
      LOG.warn("{}: Failed to decide whether to checkpoint from the journal reader with error {}.",
          mMaster.getName(), e.getMessage());
      return;
    }

    LOG.info("{}: Starting to checkpoint to sequence number {}.", mMaster.getName(),
        mJournalReader.getNextSequenceNumber());

    Iterator<alluxio.proto.journal.Journal.JournalEntry> it = mMaster.iterator();
    JournalWriter journalWriter = null;
    IOException exception = null;
    try {
      journalWriter = mJournal.getWriter(JournalWriterCreateOptions.defaults().setPrimary(false)
          .setNextSequenceNumber(mJournalReader.getNextSequenceNumber()));
      while (it.hasNext() && !mInitiateShutdown) {
        journalWriter.write(it.next());
      }
    } catch (IOException e) {
      LOG.warn("{}: Failed to checkpoint with error {}.", mMaster.getName(), e.getMessage());
      exception = e;
    }

    if (journalWriter != null) {
      try {
        if (it.hasNext() || mInitiateShutdown || exception != null) {
          journalWriter.cancel();
          LOG.info("{}: Cancelled writing checkpoint to sequence number {}: {} {} {}.",
              mMaster.getName(), mJournalReader.getNextSequenceNumber(), it.hasNext(),
              mInitiateShutdown, exception != null ? exception.getMessage() : "null");
        } else {
          journalWriter.close();
          LOG.info("{}: Done with writing checkpoint to sequence number {}.", mMaster.getName(),
              mJournalReader.getNextSequenceNumber());
        }
      } catch (IOException e) {
        LOG.warn("{}: Failed to cancel or close the checkpoint with error {}.", mMaster.getName(),
            e.getMessage());
      }
    }
  }
}
