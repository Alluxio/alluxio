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
import alluxio.ProcessUtils;
import alluxio.PropertyKey;
import alluxio.exception.JournalClosedException;
import alluxio.exception.status.UnavailableException;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeoutRetry;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Context for storing master journal information.
 */
@NotThreadSafe
public final class MasterJournalContext implements JournalContext {
  private static final Logger LOG = LoggerFactory.getLogger(MasterJournalContext.class);
  private static final long INVALID_FLUSH_COUNTER = -1;
  private static final long FLUSH_RETRY_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS);
  private static final int FLUSH_RETRY_INTERVAL_MS =
      (int) Configuration.getMs(PropertyKey.MASTER_JOURNAL_FLUSH_RETRY_INTERVAL);

  private final AsyncJournalWriter mAsyncJournalWriter;
  private long mFlushCounter;

  /**
   * Constructs a {@link MasterJournalContext}.
   *
   * @param asyncJournalWriter a {@link AsyncJournalWriter}
   */
  public MasterJournalContext(AsyncJournalWriter asyncJournalWriter) {
    Preconditions.checkNotNull(asyncJournalWriter, "asyncJournalWriter");
    mAsyncJournalWriter = asyncJournalWriter;
    mFlushCounter = INVALID_FLUSH_COUNTER;
  }

  @Override
  public void append(JournalEntry entry) {
    mFlushCounter = mAsyncJournalWriter.appendEntry(entry);
  }

  /**
   * Waits for the flush counter to be flushed to the journal. If the counter is
   * {@link #INVALID_FLUSH_COUNTER}, this is a noop.
   */
  private void waitForJournalFlush() throws UnavailableException {
    if (mFlushCounter == INVALID_FLUSH_COUNTER) {
      // Check this before the precondition.
      return;
    }

    RetryPolicy retry = new TimeoutRetry(FLUSH_RETRY_TIMEOUT_MS, FLUSH_RETRY_INTERVAL_MS);
    while (retry.attempt()) {
      try {
        mAsyncJournalWriter.flush(mFlushCounter);
        return;
      } catch (IOException e) {
        LOG.warn("Journal flush failed. retrying...", e);
      } catch (JournalClosedException e) {
        throw new UnavailableException(String.format("Failed to complete request: %s",
            e.getMessage()), e);
      } catch (Throwable e) {
        ProcessUtils.fatalError(LOG, e, "Journal flush failed");
      }
    }
    ProcessUtils.fatalError(LOG, "Journal flush failed after %d attempts", retry.getAttemptCount());
  }

  @Override
  public void close() throws UnavailableException {
    waitForJournalFlush();
  }
}
