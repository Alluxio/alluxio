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

import alluxio.ProcessUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.Master;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.sink.JournalSink;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.util.CommonUtils;
import alluxio.util.ExceptionUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This thread continually replays the journal and applies it to the master, until the master
 * initiates the shutdown of the thread.
 *
 * It periodically creates checkpoints. When the thread is stopped while it is writing checkpoint,
 * the checkpoint being written will be cancelled.
 *
 * Once awaitTermination is called, the thread will ignore InterruptedException and shut down like
 * normal (replaying all completed journal logs and waiting for a quiet period to elapse).
 */
@NotThreadSafe
public final class UfsJournalCheckpointThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalCheckpointThread.class);

  /** The master to apply the journal entries to. */
  private final Master mMaster;
  /** The journal. */
  private final UfsJournal mJournal;
  /** Make sure no new journal logs are found for this amount of time before shutting down. */
  private final long mShutdownQuietWaitTimeMs;
  /** If not journal log is found, sleep for this amount of time and check again. */
  private final int mJournalCheckpointSleepTimeMs;
  /** Writes a new checkpoint after processing this many journal entries. */
  private final long mCheckpointPeriodEntries;
  /** Object for sycnhronizing accesses to mCheckpointing. */
  private final Object mCheckpointingLock = new Object();
  /** Whether we are currently creating a checkpoint. */
  @GuardedBy("mCheckpointingLock")
  private boolean mCheckpointing = false;
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

  /** A supplier of journal sinks for this journal. */
  private final Supplier<Set<JournalSink>> mJournalSinks;

  /** The last sequence number applied to the journal. */
  private volatile long mLastAppliedSN;

  /**
   * The state of the journal catchup.
   */
  public enum CatchupState {
    NOT_STARTED, IN_PROGRESS, DONE;
  }

  private volatile CatchupState mCatchupState = CatchupState.NOT_STARTED;

  /**
   * Creates a new instance of {@link UfsJournalCheckpointThread}.
   *
   * @param master the master to apply the journal entries to
   * @param journal the journal
   * @param journalSinks a supplier for journal sinks
   */
  public UfsJournalCheckpointThread(Master master, UfsJournal journal,
      Supplier<Set<JournalSink>> journalSinks) {
    this(master, journal, 0L, journalSinks);
  }

  /**
   * Creates a new instance of {@link UfsJournalCheckpointThread}.
   *
   * @param master the master to apply the journal entries to
   * @param journal the journal
   * @param startSequence the journal start sequence
   * @param journalSinks a supplier for journal sinks
   */
  public UfsJournalCheckpointThread(Master master, UfsJournal journal, long startSequence,
      Supplier<Set<JournalSink>> journalSinks) {
    mMaster = Preconditions.checkNotNull(master, "master");
    mJournal = Preconditions.checkNotNull(journal, "journal");
    mShutdownQuietWaitTimeMs = journal.getQuietPeriodMs();
    mJournalCheckpointSleepTimeMs =
        (int) ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS);
    mJournalReader = new UfsJournalReader(mJournal, startSequence, false);
    mCheckpointPeriodEntries =
        ServerConfiguration.getLong(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES);
    mJournalSinks = journalSinks;
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
    synchronized (mCheckpointingLock) {
      if (mCheckpointing) {
        interrupt();
      }
    }

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
    mStopped = true;
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
    // Start a new thread which tracks the journal replay statistics
    ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 30000, Integer.MAX_VALUE);
    long start = System.currentTimeMillis();
    final OptionalLong finalSN = UfsJournalReader.getLastSN(mJournal);
    Thread t = new Thread(() -> {
      UfsJournalProgressLogger progressLogger =
          new UfsJournalProgressLogger(mJournal, finalSN, () -> mLastAppliedSN);
      while (!Thread.currentThread().isInterrupted() && retry.attempt()) {
        // log current stats
        progressLogger.logProgress();
      }
    });
    try {
      t.start();
      runInternal();
    } catch (Throwable e) {
      t.interrupt();
      ProcessUtils.fatalError(LOG, e, "%s: Failed to run journal checkpoint thread, crashing.",
          mMaster.getName());
      System.exit(-1);
    } finally {
      t.interrupt();
      try {
        t.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("interrupted while waiting for journal stats thread to shut down.");
      }
    }
  }

  private void runInternal() {
    // Keeps reading journal entries. If none is found, sleep for sometime. Periodically write
    // checkpoints if some conditions are met. When a shutdown signal is received, wait until
    // no new journal entries.

    LOG.info("{}: Journal checkpoint thread started.", mMaster.getName());
    // Set to true if it has waited for a quiet period. Reset if a valid journal entry is read.
    boolean quietPeriodWaited = false;
    mCatchupState = CatchupState.IN_PROGRESS;
    while (true) {
      JournalEntry entry = null;
      try {
        switch (mJournalReader.advance()) {
          case CHECKPOINT:
            LOG.debug("{}: Restoring from checkpoint", mMaster.getName());
            mMaster.restoreFromCheckpoint(mJournalReader.getCheckpoint());
            LOG.debug("{}: Finished restoring from checkpoint", mMaster.getName());
            break;
          case LOG:
            entry = mJournalReader.getEntry();
            try {
              if (!mMaster.processJournalEntry(entry)) {
                JournalUtils
                    .handleJournalReplayFailure(LOG, null, "%s: Unrecognized journal entry: %s",
                        mMaster.getName(), entry);
              } else {
                JournalUtils.sinkAppend(mJournalSinks, entry);
              }
            } catch (Throwable t) {
              JournalUtils.handleJournalReplayFailure(LOG, t,
                  "%s: Failed to read or process journal entry %s.", mMaster.getName(), entry);
            }
            if (quietPeriodWaited) {
              LOG.info("Quiet period interrupted by new journal entry");
              quietPeriodWaited = false;
            }
            mLastAppliedSN = entry.getSequenceNumber();
            break;
          default:
            mCatchupState = CatchupState.DONE;
            break;
        }
      } catch (IOException e) {
        LOG.error("{}: Failed to read or process a journal entry.", mMaster.getName(), e);
        try {
          mJournalReader.close();
        } catch (IOException ee) {
          LOG.warn("{}: Failed to close the journal reader with error {}.", mMaster.getName(),
              ee.toString());
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
            mCatchupState = CatchupState.DONE;
            LOG.info("{}: Journal checkpoint thread has been shutdown. No new logs have been found "
                + "during the quiet period.", mMaster.getName());
            if (mJournalReader != null) {
              try {
                mJournalReader.close();
              } catch (IOException e) {
                LOG.warn("{}: Failed to close the journal reader with error {}.", mMaster.getName(),
                    e.toString());
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
      if (Thread.interrupted() && !mShutdownInitiated) {
        LOG.info("{}: Checkpoint thread interrupted, shutting down", mMaster.getName());
        return;
      }
    }
  }

  /**
   * @return the state of the master process journal catchup
   */
  public CatchupState getCatchupState() {
    return mCatchupState;
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
          mMaster.getName(), e.toString());
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
      UfsJournalCheckpointWriter journalWriter =
          mJournal.getCheckpointWriter(nextSequenceNumber);
      try {
        synchronized (mCheckpointingLock) {
          if (mShutdownInitiated) {
            // This checkpoint thread is signaled to shutdown, so any checkpoint in progress must be
            // canceled/invalidated.
            journalWriter.cancel();
            return;
          }
          mCheckpointing = true;
        }
        mMaster.writeToCheckpoint(journalWriter);
      } catch (Throwable t) {
        if (ExceptionUtils.containsInterruptedException(t)) {
          Thread.currentThread().interrupt();
        } else {
          LOG.error("{}: Failed to create checkpoint", mMaster.getName(), t);
        }
        journalWriter.cancel();
        LOG.info("{}: Cancelled checkpoint [sequence number {}].", mMaster.getName(),
            nextSequenceNumber);
        return;
      } finally {
        synchronized (mCheckpointingLock) {
          mCheckpointing = false;
        }
        // If shutdown has been initiated, we assume that the interrupt was just intended to break
        // out of writeToCheckpoint early. We complete an orderly shutdown instead of stopping the
        // thread early.
        if (Thread.interrupted() && !mShutdownInitiated) {
          LOG.warn("{}: Checkpoint was interrupted but shutdown has not be initiated",
              mMaster.getName());
          Thread.currentThread().interrupt();
        }
        journalWriter.close();
      }
      LOG.info("{}: Finished checkpoint [sequence number {}].", mMaster.getName(),
          nextSequenceNumber);
      mNextSequenceNumberToCheckpoint = nextSequenceNumber;
    } catch (IOException e) {
      LOG.error("{}: Failed to checkpoint.", mMaster.getName(), e);
    }
  }
}
