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

package alluxio.master.journal.raft;

import alluxio.ProcessUtils;
import alluxio.master.journal.AbstractCatchupThread;
import alluxio.master.journal.CatchupFuture;
import alluxio.master.journal.JournalEntryAssociation;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.sink.JournalSink;
import alluxio.proto.journal.Journal;
import alluxio.resource.LockResource;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Used to control applying to masters.
 *
 * It is supported by this class to suspend and resume applies, without blocking the callers.
 * During suspend, entries will be buffered in-memory in order to not block caller
 * when it wants new entries to be processed. These buffered entries will be applied to masters
 * when this applier is resumed.
 *
 * TODO(ggezer): Extend with on-disk buffering.
 */
@ThreadSafe
public class BufferedJournalApplier {
  private static final Logger LOG = LoggerFactory.getLogger(BufferedJournalApplier.class);

  /** Resume will acquire exclusive lock when buffer has less than this value. */
  private static final int RESUME_LOCK_BUFFER_SIZE_WATERMARK = 100;

  /** Resume will acquire exclusive lock when can't finish after this much time. */
  private static final int RESUME_LOCK_TIME_LIMIT_MS = 30000;

  /** Journals managed by this applier. */
  private final Map<String, RaftJournal> mJournals;
  /** A supplier of journal sinks for this applier. */
  private final Supplier<Set<JournalSink>> mJournalSinks;

  /** The last sequence applied. */
  private long mLastAppliedSequence = -1;

  /** Whether this state machine is suspended. */
  @GuardedBy("mStateLock")
  private boolean mSuspended = false;

  /** Whether a resume() call is in progress. */
  @GuardedBy("mStateLock")
  private boolean mResumeInProgress = false;

  /** Buffer to use during suspend period. */
  private Queue<Journal.JournalEntry> mSuspendBuffer = new ConcurrentLinkedQueue();

  /** Used to store latest catch-up thread. */
  private AbstractCatchupThread mCatchupThread;

  /** Used to synchronize buffer state. */
  private ReentrantLock mStateLock = new ReentrantLock(true);

  /**
   * Creates a buffered applier over given journals.
   *
   * @param journals journals
   * @param journalSinks journal sinks
   */
  public BufferedJournalApplier(Map<String, RaftJournal> journals,
      Supplier<Set<JournalSink>> journalSinks) {
    mJournals = journals;
    mJournalSinks = journalSinks;
  }

  /**
   * Processes given journal entry for applying. Entry could be applied or buffered based on
   * buffer's state.
   *
   * @param journalEntry the journal entry
   */
  public void processJournalEntry(Journal.JournalEntry journalEntry) {
    try (LockResource stateLock = new LockResource(mStateLock)) {
      if (mSuspended) {
        // New entry submissions are monitored by catch-up threads.
        synchronized (mSuspendBuffer) {
          mSuspendBuffer.offer(journalEntry);
          mSuspendBuffer.notifyAll();
        }
      } else {
        applyToMaster(journalEntry);
      }
    }
  }

  /**
   * @return {@code true} if this applier was suspended
   */
  public boolean isSuspended() {
    try (LockResource stateLock = new LockResource(mStateLock)) {
      return mSuspended;
    }
  }

  /**
   * Suspend the applier.
   *
   * After this call, journal entries will be buffered until {@link #resume()} or
   * {@link #catchup(long)} is called.
   *
   * @throws IOException
   */
  public void suspend() throws IOException {
    try (LockResource stateLock = new LockResource(mStateLock)) {
      Preconditions.checkState(!mSuspended, "Already suspended");
      mSuspended = true;
      LOG.info("Suspended state machine at sequence: {}", mLastAppliedSequence);
    }
  }

  /**
   * Resumes the applier. This method will apply all buffered entries before returning.
   *
   * @throws IOException
   */
  public void resume() throws IOException {
    try (LockResource stateLock = new LockResource(mStateLock)) {
      Preconditions.checkState(mSuspended, "Not suspended");
      Preconditions.checkState(!mResumeInProgress, "Resume in progress");
      mResumeInProgress = true;
      LOG.info("Resuming state machine from sequence: {}", mLastAppliedSequence);
    }

    cancelCatchup();

    /**
     * Applies all buffered entries.
     *
     * It doesn't block state until
     *   -> buffer contains few elements ( RESUME_LOCK_BUFFER_SIZE_WATERMARK )
     *   -> was running for a long time  ( RESUME_LOCK_TIME_LIMIT_MS         )
     */
    try {
      // Mark resume start time.
      long resumeStartTimeMs = System.currentTimeMillis();
      // Lock initially if few or none elements in the queue.
      if (mSuspendBuffer.size() <= RESUME_LOCK_BUFFER_SIZE_WATERMARK) {
        mStateLock.lock();
      }

      while (!mSuspendBuffer.isEmpty()) {
        applyToMaster(mSuspendBuffer.remove());

        // Check whether to lock the state now.
        boolean lockSubmission = !mStateLock.isHeldByCurrentThread()
            && (mSuspendBuffer.size() <= RESUME_LOCK_BUFFER_SIZE_WATERMARK
                || (System.currentTimeMillis() - resumeStartTimeMs) > RESUME_LOCK_TIME_LIMIT_MS);

        if (lockSubmission) {
          mStateLock.lock();
        }
      }
    } finally {
      mSuspended = false;
      mResumeInProgress = false;
      mCatchupThread = null;
      mStateLock.unlock();
    }
  }

  private void cancelCatchup() {
    // Cancel catching up thread if active.
    if (mCatchupThread != null && mCatchupThread.isAlive()) {
      mCatchupThread.cancel();
      mCatchupThread.waitTermination();
    }
  }

  /**
   * Initiates catching up of the applier to a target sequence.
   * This method leaves the applier in suspended state.
   *
   * @param sequence target sequence
   * @return the future to track when applier reaches the target sequence
   */
  public CatchupFuture catchup(long sequence) {
    try (LockResource stateLock = new LockResource(mStateLock)) {
      Preconditions.checkState(mSuspended, "Not suspended");
      Preconditions.checkState(!mResumeInProgress, "Resume in progress");
      Preconditions.checkState(mCatchupThread == null || !mCatchupThread.isAlive(),
          "Catch-up task in progress.");
      Preconditions.checkState(sequence >= 0, "Invalid negative sequence: %d", sequence);
      Preconditions.checkState(mLastAppliedSequence <= sequence,
          "Can't catchup to past. Current: %d, Requested: %d", mLastAppliedSequence, sequence);
      LOG.info("Catching up state machine to sequence: {}", sequence);

      // Complete the request if already at target sequence.
      if (mLastAppliedSequence == sequence) {
        return CatchupFuture.completed();
      }

      // Create an async task for catching up to target sequence.
      mCatchupThread = new RaftJournalCatchupThread(sequence);
      mCatchupThread.start();
      return new CatchupFuture(mCatchupThread);
    }
  }

  /**
   * Applies the entry to master and updates last applied sequence.
   * Calls to it should be serialized.
   */
  private void applyToMaster(Journal.JournalEntry entry) {
    String masterName;
    try {
      masterName = JournalEntryAssociation.getMasterForEntry(entry);
    } catch (Exception t) {
      ProcessUtils.fatalError(LOG, t, "Unrecognized journal entry: %s", entry);
      throw new IllegalStateException();
    }
    try {
      Journaled master = mJournals.get(masterName).getStateMachine();
      LOG.trace("Applying entry to master {}: {} ", masterName, entry);
      master.processJournalEntry(entry);
      JournalUtils.sinkAppend(mJournalSinks, entry);
    } catch (Exception t) {
      JournalUtils.handleJournalReplayFailure(LOG, t,
          "Failed to apply journal entry to master %s. Entry: %s", masterName, entry);
    }
    // Store last applied sequence.
    mLastAppliedSequence = entry.getSequenceNumber();
  }

  /**
   * Resets the suspend applier. Should only be used when the state machine is reset.
   */
  public void close() {
    try (LockResource stateLock = new LockResource(mStateLock)) {
      cancelCatchup();
      mSuspendBuffer.clear();
    }
  }

  /**
   * RAFT implementation for {@link AbstractCatchupThread}.
   */
  class RaftJournalCatchupThread extends AbstractCatchupThread {
    /** Where to stop catching up. */
    private long mCatchUpEndSequence;
    /** Used to stop catching up early. */
    private boolean mStopCatchingUp = false;

    /**
     * Creates RAFT journal catch-up thread.
     *
     * @param sequence end sequence(inclusive)
     */
    public RaftJournalCatchupThread(long sequence) {
      mCatchUpEndSequence = sequence;
      setName("raft-catchup-thread");
    }

    @Override
    public void cancel() {
      // Signal thread to bail early.
      synchronized (mSuspendBuffer) {
        mStopCatchingUp = true;
        mSuspendBuffer.notifyAll();
      }
    }

    protected void runCatchup() {
      // Spin for catching up until cancelled.
      while (!mStopCatchingUp && mLastAppliedSequence < mCatchUpEndSequence) {
        // Wait until notified for cancellation or more entries.
        synchronized (mSuspendBuffer) {
          while (!mStopCatchingUp && mSuspendBuffer.size() == 0) {
            try {
              mSuspendBuffer.wait();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException("Interrupted while catching up.");
            }
          }

          // Catch up as much as possible.
          while (!mSuspendBuffer.isEmpty() && mLastAppliedSequence < mCatchUpEndSequence) {
            applyToMaster(mSuspendBuffer.remove());
          }
        }
      }
    }
  }
}
