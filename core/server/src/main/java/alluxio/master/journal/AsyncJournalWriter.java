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
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public final class AsyncJournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final JournalWriter mJournalWriter;
  private final ConcurrentLinkedQueue<JournalEntry> mQueue;
  private final AtomicLong mCounter;
  private final AtomicLong mFlushCounter;

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
  }

  /**
   * Appends a {@link JournalEntry} for writing to the journal.
   *
   * @param entry the {@link JournalEntry} to append
   * @return a counter for the entry, for flushing
   */
  public long appendEntry(JournalEntry entry) {
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
      long flushCounter = mFlushCounter.get();
      if (counter <= flushCounter) {
        return;
      }
      while (counter > flushCounter) {
        for (;;) {
          JournalEntry entry = mQueue.poll();
          if (entry == null) {
            // No more entries in the queue.
            break;
          }
          mJournalWriter.getEntryOutputStream().writeEntry(entry);
          flushCounter++;

          // TODO(gpang): limit the amount of time for writing.
        }
      }
      mJournalWriter.getEntryOutputStream().flush();
      mFlushCounter.set(flushCounter);
    } finally {
      mFlushLock.unlock();
    }
  }

}
