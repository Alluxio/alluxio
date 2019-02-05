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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.JournalClosedException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.Status;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.LockResource;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This enables async journal writing, as well as some batched journal flushing.
 */
@ThreadSafe
public final class AsyncJournalWriter {
  /**
   * Used to manage and keep track of pending callers of ::flush.
   */
  private class FlushTicket {
    private long mTargetCounter;
    private SettableFuture<Void> mIsCompleted;

    public FlushTicket(long targetCounter) {
      mTargetCounter = targetCounter;
      mIsCompleted = SettableFuture.create();
    }

    public long getTargetCounter() {
      return mTargetCounter;
    }

    public void setCompleted() {
      mIsCompleted.set(null);
    }

    public void setError(Throwable exc) {
      mIsCompleted.setException(exc);
    }

    public void waitCompleted() throws InterruptedException, ExecutionException {
      mIsCompleted.get();
    }
  }

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
   * List of flush tickets submitted by ::flush() method.
   */
  @GuardedBy("mTicketLock")
  private final List<FlushTicket> mTicketList = new ArrayList<>(200);

  /**
   * Used to guard access to ticket cache.
   */
  private final ReentrantLock mTicketLock = new ReentrantLock(true);

  /**
   * Dedicated thread for writing and flushing entries in journal queue.
   * It goes over the {@code mTicketList} after every flush session and releases waiters.
   */
  private final Thread mFlushThread = new Thread(this::flushProc);

  /**
   * Creates a {@link AsyncJournalWriter}.
   *
   * @param journalWriter a journal writer to write to
   */
  public AsyncJournalWriter(JournalWriter journalWriter) {
    mJournalWriter = Preconditions.checkNotNull(journalWriter, "journalWriter");
    mQueue = new ConcurrentLinkedQueue<>();
    mCounter = new AtomicLong(0);
    mFlushCounter = new AtomicLong(0);
    mWriteCounter = new AtomicLong(0);
    mFlushBatchTimeNs =
            1000000L * ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS);
    mFlushThread.start();
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
   * Closes the async writer.
   * PS: It initiates but does not wait for outstanding entries to be flushed.
   */
  public void close() {
    if (mFlushThread != null) {
      mFlushThread.interrupt();
    }
  }

  /**
   * A dedicated thread that goes over outstanding queue items and writes/flushes them. Other
   * threads can track progress by submitting tickets via ::flush() call.
   */
  private void flushProc() {
    /**
     * Runs the loop until thread is interrupted.
     */
    boolean exit = false;
    while (!exit) {

      /**
       * Wait to be woken up by flush requests as long as the queue is empty. Bail and drain the
       * queue, if interrupted.
       */
      synchronized (mFlushThread) {
        while (mQueue.isEmpty()) {
          try {
            mFlushThread.wait();
          } catch (InterruptedException ie) {
            // Time to exit.
            exit = true;
            break;
          }
        }
      }

      try {
        long writeCounter = mWriteCounter.get();
        long startTime = System.nanoTime();
        boolean needFlush = false;

        /**
         * Write pending entries to journal.
         */
        while (!mQueue.isEmpty()) {
          // Get, but do not remove, the head entry.
          JournalEntry entry = mQueue.peek();
          if (entry == null) {
            // No more entries in the queue. Break write session.
            break;
          }
          mJournalWriter.write(entry);
          needFlush = true;
          // Remove the head entry, after the entry was successfully written.
          mQueue.poll();
          writeCounter = mWriteCounter.incrementAndGet();

          if ((System.nanoTime() - startTime) >= mFlushBatchTimeNs) {
            // This thread has been writing to the journal for enough time. Break out of the
            // infinite while-loop.
            break;
          }
        }

        if (needFlush) {
          mJournalWriter.flush();
          mFlushCounter.set(writeCounter);

          /**
           * Notify tickets that have been served to wake up.
           */
          List<FlushTicket> closedTickets = new ArrayList<>();
          try (LockResource lr = new LockResource(mTicketLock)) {
            for (FlushTicket ticket : mTicketList) {
              if (ticket.getTargetCounter() <= writeCounter) {
                ticket.setCompleted();
                closedTickets.add(ticket);
              }
            }
            mTicketList.removeAll(closedTickets);
          }
        }
      } catch (IOException | JournalClosedException exc) {
        /**
         * Release only tickets that have been flushed. Fail the rest.
         */
        List<FlushTicket> closedTickets = new ArrayList<>();
        try (LockResource lr = new LockResource(mTicketLock)) {
          for (FlushTicket ticket : mTicketList) {
            closedTickets.add(ticket);
            if (ticket.getTargetCounter() <= mFlushCounter.get()) {
              ticket.setCompleted();
            } else {
              ticket.setError(exc);
            }
          }
          mTicketList.removeAll(closedTickets);
        }
      }
    }
  }

  /**
   * Submits a ticket to flush thread and waits until ticket is served.
   *
   * If the specified counter is already flushed, this is essentially a no-op.
   *
   * @param targetCounter the counter to flush
   */
  @SuppressWarnings("Duplicates")
  public void flush(final long targetCounter) throws IOException, JournalClosedException {
    // Return if flushed.
    if (targetCounter <= mFlushCounter.get()) {
      return;
    }

    /**
     * Submit the ticket for flush thread to process.
     */
    FlushTicket ticket = new FlushTicket(targetCounter);
    try (LockResource lr = new LockResource(mTicketLock)) {
      mTicketList.add(ticket);
    }

    try {
      /**
       * Notify the flush thread. It will have no effect if there is no ticket left to process,
       * which means this ticket has been served as well.
       */
      synchronized (mFlushThread) {
        mFlushThread.notify();
      }

      /**
       * Wait on the ticket until completed.
       */
      ticket.waitCompleted();
    } catch (InterruptedException ie) {
      /**
       * Interpret interruption as cancellation.
       */
      throw new AlluxioStatusException(Status.CANCELED, ie);
    } catch (ExecutionException ee) {
      /**
       * Filter, journal specific exception codes.
       */
      Throwable cause = ee.getCause();
      if (cause != null && cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause != null && cause instanceof JournalClosedException) {
        throw (JournalClosedException) cause;
      }
      // Not expected. throw internal error.
      throw new AlluxioStatusException(Status.INTERNAL, ee);
    }
  }
}
