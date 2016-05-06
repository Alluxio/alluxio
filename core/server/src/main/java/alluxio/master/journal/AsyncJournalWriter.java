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

import alluxio.Constants;
import alluxio.master.MasterContext;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This enables async journal writing, as well as some batched journal flushing.
 */
@ThreadSafe
public final class AsyncJournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** An invalid journal flush counter. */
  public static final long INVALID_FLUSH_COUNTER = -1;

  private final JournalWriter mJournalWriter;
  private final ConcurrentLinkedQueue<JournalEntry> mQueue;
  private final AtomicLong mCounter;
  private final AtomicLong mFlushCounter;
  private final long mFlushBatchTime;

  /**
   * Use a {@link ReentrantLock} to guard the journal writing. Using the fairness policy seems to
   * result in better throughput.
   */
  private final ReentrantLock mFlushLock = new ReentrantLock(true);

  /**
   * Creates a {@link AsyncJournalWriter}.
   *
   * @param journalWriter the {@link JournalWriter} to use for writing
   */
  public AsyncJournalWriter(JournalWriter journalWriter) {
    mJournalWriter = Preconditions.checkNotNull(journalWriter);
    mQueue = new ConcurrentLinkedQueue<>();
    mCounter = new AtomicLong(0);
    mFlushCounter = new AtomicLong(0);
    mFlushBatchTime = MasterContext.getConf().getLong(Constants.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS);
  }

  /**
   * Appends a {@link JournalEntry} for writing to the journal.
   *
   * @param entry the {@link JournalEntry} to append
   * @return a counter for the entry, for flushing
   */
  public long appendEntry(JournalEntry entry) {
    // TODO(gpang): handle bounding the queue if it becomes too large.

    /**
     * Protocol for appending entries
     *
     * This protocol is lock free, to reduce the overhead in critical sections. It uses
     * {@link AtomicLong} and {@link ConcurrentLinkedQueue} which are both lock-free.
     *
     * The invariant that must be satisfied is that the 'counter' that is returned must be
     * greater than or equal to the actual counter of the entry in the queue.
     *
     * In order to guarantee the invariant, the {@link #mCounter} is incremented before adding the
     * entry to the {@link #mQueue}. AFTER the counter is incremented, whenever the counter is
     * read, it is guaranteed to be greater than or equal to the counter for the queue entries.
     *
     * Therefore, the {@link #mCounter} must be read AFTER the entry is added to the queue. The
     * resulting read of the counter AFTER the entry is added is guaranteed to be greater than or
     * equal to the counter for the entries in the queue.
     */
    mCounter.incrementAndGet();
    mQueue.offer(entry);
    return mCounter.get();
  }

  /**
   * Flushes and waits until the specified counter is flushed to the journal. If the specified
   * counter is already flushed, this is essentially a no-op.
   *
   * @param counter the counter to flush
   * @throws IOException if an error occurs in flushing the journal
   */
  public void flush(final long counter) throws IOException {
    if (counter <= mFlushCounter.get()) {
      return;
    }
    // Using reentrant lock, since it seems to result in higher throughput than using 'synchronized'
    mFlushLock.lock();
    try {
      long startTime = System.currentTimeMillis();
      long flushCounter = mFlushCounter.get();
      if (counter <= flushCounter) {
        // The specified counter is already flushed, so just return.
        return;
      }
      while (counter > flushCounter) {
        for (;;) {
          JournalEntry entry = mQueue.poll();
          if (entry == null) {
            // No more entries in the queue. Break out of the infinite for-loop.
            break;
          }
          mJournalWriter.getEntryOutputStream().writeEntry(entry);
          flushCounter++;

          if (flushCounter >= counter) {
            if ((System.currentTimeMillis() - startTime) >= mFlushBatchTime) {
              // This thread has been writing to the journal for enough time. Break out of the
              // infinite for-loop.
              break;
            }
          }
        }
      }
      mJournalWriter.getEntryOutputStream().flush();
      mFlushCounter.set(flushCounter);
    } finally {
      mFlushLock.unlock();
    }
  }

}
