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
public final class JournalCheckpointerThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(JournalCheckpointerThread.class);

  /** The master to apply the journal entries to. */
  private final Master mMaster;
  /** The journal to tail. */
  private final Journal mJournal;
  private final int mShutdownQuietWaitTimeMs;
  private final int mJournalCheckpointerSleepTimeMs;
  /** This becomes true when the master initiates the shutdown. */
  private volatile boolean mInitiateShutdown = false;

  private JournalReader mJournalReader;

  /** True if this thread is no longer running. */
  private boolean mStopped = false;

  /**
   * Creates a new instance of {@link JournalCheckpointerThread}.
   *
   * @param master the master to apply the journal entries to
   * @param journal the journal to tail
   */
  public JournalCheckpointerThread(Master master, Journal journal) {
    mMaster = Preconditions.checkNotNull(master);
    mJournal = Preconditions.checkNotNull(journal);
    mShutdownQuietWaitTimeMs =
        Configuration.getInt(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS);
    mJournalCheckpointerSleepTimeMs =
        Configuration.getInt(PropertyKey.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS);
    mJournalReader = mJournal.getReader(JournalReaderCreateOptions.defaults().setPrimary(false));
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
   * @return the {@link JournalCheckpointer} that this thread last used to tail the journal. This will
   *         only return the {@link JournalCheckpointer} if this thread is no longer running, to prevent
   *         concurrent access to the {@link JournalCheckpointer}. Returns null if this thread has not yet
   *         used a {@link JournalCheckpointer}, or if this thread is still running.
   */
  public JournalReader getJournalReader() {
    if (mStopped) {
      return mJournalReader;
    }
    return null;
  }

  void maybeCheckpoint() {
    if (mInitiateShutdown) {
      return;
    }
    try {
      if (!mJournalReader.shouldCheckpoint()) {
        return;
      }
    } catch (IOException e) {
      LOG.warn("Failed to decide whether to checkpoint from the journal reader with error {}.", e.getMessage());
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

    LOG.info("{}: Journal checkpointer started.", mMaster.getName());
    alluxio.proto.journal.Journal.JournalEntry entry;
    while (!mInitiateShutdown) {
      // The start time (ms) for the initiated shutdown.
      try {
        entry = mJournalReader.read();
        mMaster.processJournalEntry(entry);
      } catch (IOException | InvalidJournalEntryException e) {
        LOG.warn("Failed to process the journal entry with error {}.", e.getMessage());
        try {
          mJournalReader.close();
        } catch (IOException ee) {
          LOG.warn("Failed to close the journal reader with error {}.", ee.getMessage());
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
        LOG.info("{}: No journal entry found. sleeping for {}ms.", mMaster.getName(),
            mJournalCheckpointerSleepTimeMs);
        CommonUtils.sleepMs(LOG, mJournalCheckpointerSleepTimeMs);
      }
    }
  }
}
