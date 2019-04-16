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

package alluxio.master.journalv0;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;

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
  private final JournalWriter mJournalWriter;
  private final ConcurrentLinkedQueue<JournalEntry> mQueue;
  /** Represents the count of entries added to the journal queue. */
  private final AtomicLong mCounter;
  /** Represents the count of entries flushed to the journal writer. */
  private final AtomicLong mFlushCounter;
  /**
   * Represents the count of entries written to the journal writer.
   * Invariant: {@code mWriteCounter >= mFlushCounter}
   */
  private final AtomicLong mWriteCounter;
  /** Maximum number of nanoseconds for a batch flush. */
  private final long mFlushBatchTimeNs;

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
    mJournalWriter = Preconditions.checkNotNull(journalWriter, "journalWriter");
    mQueue = new ConcurrentLinkedQueue<>();
    mCounter = new AtomicLong(0);
    mFlushCounter = new AtomicLong(0);
    mWriteCounter = new AtomicLong(0);
    // convert milliseconds to nanoseconds.
    mFlushBatchTimeNs =
        1000000L * ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS);
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
   * @param targetCounter the counter to flush
   */
  public void flush(final long targetCounter) throws IOException {
    if (targetCounter <= mFlushCounter.get()) {
      return;
    }
    // Using reentrant lock, since it seems to result in higher throughput than using 'synchronized'
    mFlushLock.lock();
    try {
      long startTime = System.nanoTime();
      long flushCounter = mFlushCounter.get();
      if (targetCounter <= flushCounter) {
        // The specified counter is already flushed, so just return.
        return;
      }
      long writeCounter = mWriteCounter.get();
      while (targetCounter > writeCounter) {
        for (;;) {
          // Get, but do not remove, the head entry.
          JournalEntry entry = mQueue.peek();
          if (entry == null) {
            // No more entries in the queue. Break out of the infinite for-loop.
            break;
          }
          mJournalWriter.write(entry);
          // Remove the head entry, after the entry was successfully written.
          mQueue.poll();
          writeCounter = mWriteCounter.incrementAndGet();

          if (writeCounter >= targetCounter) {
            if ((System.nanoTime() - startTime) >= mFlushBatchTimeNs) {
              // This thread has been writing to the journal for enough time. Break out of the
              // infinite for-loop.
              break;
            }
          }
        }
      }
      mJournalWriter.flush();
      mFlushCounter.set(writeCounter);
    } finally {
      mFlushLock.unlock();
    }
  }
}
