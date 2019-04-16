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
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.LockResource;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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
    private final long mTargetCounter;
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
   * This counter is only accessed by the dedicated journal thread.
   * Invariant: {@code mWriteCounter >= mFlushCounter}
   */
  private Long mWriteCounter;
  /** Maximum number of nanoseconds for a batch flush. */
  private final long mFlushBatchTimeNs;

  /**
   * List of flush tickets submitted by ::flush() method.
   */
  @GuardedBy("mTicketLock")
  private final List<FlushTicket> mTicketList = new LinkedList<>();

  /**
   * Used to guard access to ticket cache.
   */
  private final ReentrantLock mTicketLock = new ReentrantLock(true);

  /**
   * Dedicated thread for writing and flushing entries in journal queue.
   * It goes over the {@code mTicketList} after every flush session and releases waiters.
   */
  private Thread mFlushThread = new Thread(this::doFlush, "AsyncJournalWriterThread");

  /**
   * Used to give permits to flush thread to start processing immediately.
   */
  private final Semaphore mFlushSemaphore = new Semaphore(0, true);

  /**
   * Control flag that is used to instruct flush thread to exit.
   */
  private volatile boolean mStopFlushing = false;

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
    mWriteCounter = 0L;
    mFlushBatchTimeNs = TimeUnit.NANOSECONDS.convert(
        ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS),
        TimeUnit.MILLISECONDS);
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
   * PS: It's not guaranteed for pending entries to be flushed.
   *     Use ::flush() for guaranteeing the entries have been flushed.
   */
  public void close() {
    stop();
  }

  @VisibleForTesting
  protected void stop() {
    // Set termination flag.
    mStopFlushing = true;
    // Give a permit for flush thread to run, in case it was blocked on permit.
    mFlushSemaphore.release();

    try {
      mFlushThread.join();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return;
    } finally {
      mFlushThread = null;
      // Try to reacquire the permit.
      mFlushSemaphore.tryAcquire();
    }
  }

  @VisibleForTesting
  protected void start() {
    if (mFlushThread != null) {
      close();
    }
    // Create a new thread.
    mFlushThread = new Thread(this::doFlush);
    // Reset termination flag before starting the new thread.
    mStopFlushing = false;
    mFlushThread.start();
  }

  /**
   * A dedicated thread that goes over outstanding queue items and writes/flushes them. Other
   * threads can track progress by submitting tickets via ::flush() call.
   */
  private void doFlush() {
    // Runs the loop until ::stop() is called.
    while (!mStopFlushing) {

      /**
       * Stand still unless;
       * - queue has items
       * - permit is given by:
       *   - clients
       *   -::stop()
       */
      while (mQueue.isEmpty() && !mStopFlushing) {
        try {
          // Wait for permit up to batch timeout.
          // PS: We don't wait for permit indefinitely in order to process
          // queued entries proactively.
          if (mFlushSemaphore.tryAcquire(mFlushBatchTimeNs, TimeUnit.NANOSECONDS)) {
            break;
          }
        } catch (InterruptedException ie) {
          break;
        }
      }

      try {
        long startTime = System.nanoTime();

        // Write pending entries to journal.
        while (!mQueue.isEmpty()) {
          // Get, but do not remove, the head entry.
          JournalEntry entry = mQueue.peek();
          if (entry == null) {
            // No more entries in the queue. Break write session.
            break;
          }
          mJournalWriter.write(entry);
          // Remove the head entry, after the entry was successfully written.
          mQueue.poll();
          mWriteCounter++;

          if (((System.nanoTime() - startTime) >= mFlushBatchTimeNs) && !mStopFlushing) {
            // This thread has been writing to the journal for enough time. Break out of the
            // infinite while-loop.
            break;
          }
        }

        // Either written new entries or previous flush had been failed.
        if (mFlushCounter.get() < mWriteCounter) {
          mJournalWriter.flush();
          mFlushCounter.set(mWriteCounter);
        }

        // Notify tickets that have been served to wake up.
        try (LockResource lr = new LockResource(mTicketLock)) {
          ListIterator<FlushTicket> ticketIterator = mTicketList.listIterator();
          while (ticketIterator.hasNext()) {
            FlushTicket ticket = ticketIterator.next();
            if (ticket.getTargetCounter() <= mFlushCounter.get()) {
              ticket.setCompleted();
              ticketIterator.remove();
            }
          }
        }
      } catch (IOException | JournalClosedException exc) {
        // Release only tickets that have been flushed. Fail the rest.
        try (LockResource lr = new LockResource(mTicketLock)) {
          ListIterator<FlushTicket> ticketIterator = mTicketList.listIterator();
          while (ticketIterator.hasNext()) {
            FlushTicket ticket = ticketIterator.next();
            ticketIterator.remove();
            if (ticket.getTargetCounter() <= mFlushCounter.get()) {
              ticket.setCompleted();
            } else {
              ticket.setError(exc);
            }
          }
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
  public void flush(final long targetCounter) throws IOException, JournalClosedException {
    // Return if flushed.
    if (targetCounter <= mFlushCounter.get()) {
      return;
    }

    // Submit the ticket for flush thread to process.
    FlushTicket ticket = new FlushTicket(targetCounter);
    try (LockResource lr = new LockResource(mTicketLock)) {
      mTicketList.add(ticket);
    }

    try {
      // Give a permit for flush thread to run.
      mFlushSemaphore.release();

      // Wait on the ticket until completed.
      ticket.waitCompleted();
    } catch (InterruptedException ie) {
      // Interpret interruption as cancellation.
      throw new AlluxioStatusException(Status.CANCELLED.withCause(ie));
    } catch (ExecutionException ee) {
      // Filter, journal specific exception codes.
      Throwable cause = ee.getCause();
      if (cause != null && cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause != null && cause instanceof JournalClosedException) {
        throw (JournalClosedException) cause;
      }
      // Not expected. throw internal error.
      throw new AlluxioStatusException(Status.INTERNAL.withCause(ee));
    } finally {
      /**
       * Client can only try to reacquire the permit it has given
       * because the permit may or may not have been used by the flush thread.
       */
      mFlushSemaphore.tryAcquire();
    }
  }
}
