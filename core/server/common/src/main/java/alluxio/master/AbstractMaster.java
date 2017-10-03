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

package alluxio.master;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Server;
import alluxio.clock.Clock;
import alluxio.exception.PreconditionMessage;
import alluxio.master.journal.AsyncJournalWriter;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalSystem;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeoutRetry;
import alluxio.util.executor.ExecutorServiceFactory;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This is the base class for all masters, and contains common functionality. Common functionality
 * mostly consists of journal operations, like initialization, journal tailing when in secondary
 * mode, or journal writing when the master is the primary.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public abstract class AbstractMaster implements Master {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMaster.class);
  private static final long INVALID_FLUSH_COUNTER = -1;
  private static final long SHUTDOWN_TIMEOUT_MS = 10 * Constants.SECOND_MS;
  private static final long JOURNAL_FLUSH_RETRY_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS);

  /** A factory for creating executor services when they are needed. */
  private ExecutorServiceFactory mExecutorServiceFactory;
  /** The executor used for running maintenance threads for the master. */
  private ExecutorService mExecutorService;
  /** A handler to the journal for this master. */
  private Journal mJournal;
  /** true if this master is in primary mode, and not secondary mode. */
  private boolean mIsPrimary = false;
  /** The {@link AsyncJournalWriter} for async journal writes. */
  private AsyncJournalWriter mAsyncJournalWriter;

  /** The clock to use for determining the time. */
  protected final Clock mClock;

  /**
   * @param journalSystem the journal system to use for tracking master operations
   * @param clock the Clock to use for determining the time
   * @param executorServiceFactory a factory for creating the executor service to use for
   *        running maintenance threads
   */
  protected AbstractMaster(JournalSystem journalSystem, Clock clock,
      ExecutorServiceFactory executorServiceFactory) {
    mJournal = journalSystem.createJournal(this);
    mClock = Preconditions.checkNotNull(clock, "clock");
    mExecutorServiceFactory =
        Preconditions.checkNotNull(executorServiceFactory, "executorServiceFactory");
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return new HashSet<>();
  }

  @Override
  public void start(Boolean isPrimary) throws IOException {
    Preconditions.checkState(mExecutorService == null);
    mExecutorService = mExecutorServiceFactory.create();
    mIsPrimary = isPrimary;
    if (mIsPrimary) {
      /**
       * The sequence for dealing with the journal before starting as the primary:
       *
       * 1. Replay the journal entries.
       * 2. Start the journal writer and optionally journal the master bootstrap states
       *    if this is a fresh start.
       *
       * Since this method is called before the master RPC server starts serving, there is no
       * concurrent access to the master during these phases.
       */

      LOG.info("{}: Starting primary master.", getName());
      mAsyncJournalWriter = new AsyncJournalWriter(mJournal);
    }
  }

  @Override
  public void stop() throws IOException {
    if (mIsPrimary) {
      LOG.info("{}: Stopping primary master.", getName());
    }
    // Shut down the executor service, interrupting any running threads.
    if (mExecutorService != null) {
      try {
        mExecutorService.shutdownNow();
        String awaitFailureMessage =
            "waiting for {} executor service to shut down. Daemons may still be running";
        try {
          if (!mExecutorService.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            LOG.warn("Timed out " + awaitFailureMessage, this.getClass().getSimpleName());
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while " + awaitFailureMessage, this.getClass().getSimpleName());
        }
      } finally {
        mExecutorService = null;
      }
    }
    LOG.info("{}: Stopped {} master.", getName(), mIsPrimary ? "primary" : "secondary");
  }

  /**
   * Writes a {@link JournalEntry} to the journal. Does NOT flush the journal.
   *
   * @param entry the {@link JournalEntry} to write to the journal
   */
  protected void writeJournalEntry(JournalEntry entry) {
    Preconditions.checkNotNull(mJournal, "Cannot write entry: journal is null.");
    try {
      mJournal.write(entry);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Flushes the journal.
   */
  protected void flushJournal() {
    Preconditions.checkNotNull(mJournal, "Cannot flush journal: journal is null.");
    try {
      mJournal.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Appends a {@link JournalEntry} for writing to the journal.
   *
   * @param entry the {@link JournalEntry}
   * @param journalContext the journal context
   */
  protected void appendJournalEntry(JournalEntry entry, JournalContext journalContext) {
    Preconditions.checkNotNull(mAsyncJournalWriter, PreconditionMessage.ASYNC_JOURNAL_WRITER_NULL);
    journalContext.append(entry);
  }

  /**
   * Waits for the flush counter to be flushed to the journal. If the counter is
   * {@link #INVALID_FLUSH_COUNTER}, this is a noop.
   *
   * @param journalContext the journal context
   */
  private void waitForJournalFlush(JournalContext journalContext) {
    if (journalContext.getFlushCounter() == INVALID_FLUSH_COUNTER) {
      // Check this before the precondition.
      return;
    }
    Preconditions.checkNotNull(mAsyncJournalWriter, PreconditionMessage.ASYNC_JOURNAL_WRITER_NULL);

    RetryPolicy retry = new TimeoutRetry(JOURNAL_FLUSH_RETRY_TIMEOUT_MS, Constants.SECOND_MS);
    while (retry.attemptRetry()) {
      try {
        mAsyncJournalWriter.flush(journalContext.getFlushCounter());
        return;
      } catch (IOException e) {
        LOG.warn("Journal flush failed. retrying...", e);
      }
    }
    LOG.error(
        "Journal flush failed after {} attempts. Terminating process to prevent inconsistency.",
        retry.getRetryCount());
    if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
      throw new RuntimeException("Journal flush failed after " + retry.getRetryCount()
          + " attempts. Terminating process to prevent inconsistency.");
    }
    System.exit(-1);
  }

  /**
   * @return the {@link ExecutorService} for this master
   */
  protected ExecutorService getExecutorService() {
    return mExecutorService;
  }

  /**
   * @return new instance of {@link JournalContext}
   */
  protected JournalContext createJournalContext() {
    return new MasterJournalContext(mAsyncJournalWriter);
  }

  /**
   * Context for storing journaling information.
   */
  @NotThreadSafe
  public final class MasterJournalContext implements JournalContext {
    private final AsyncJournalWriter mAsyncJournalWriter;
    private long mFlushCounter;

    /**
     * Constructs a {@link MasterJournalContext}.
     *
     * @param asyncJournalWriter a {@link AsyncJournalWriter}
     */
    private MasterJournalContext(AsyncJournalWriter asyncJournalWriter) {
      mAsyncJournalWriter = asyncJournalWriter;
      mFlushCounter = INVALID_FLUSH_COUNTER;
    }

    @Override
    public long getFlushCounter() {
      return mFlushCounter;
    }

    @Override
    public void append(JournalEntry entry) {
      if (mAsyncJournalWriter != null) {
        mFlushCounter = mAsyncJournalWriter.appendEntry(entry);
      }
    }

    @Override
    public void close() {
      if (mAsyncJournalWriter != null) {
        waitForJournalFlush(this);
      }
    }
  }
}
