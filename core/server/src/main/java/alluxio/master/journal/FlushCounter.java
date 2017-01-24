package alluxio.master.journal;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.PreconditionMessage;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeoutRetry;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class FlushCounter implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final long JOURNAL_FLUSH_RETRY_TIMEOUT_MS =
      Configuration.getLong(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS);

  private AsyncJournalWriter mAsyncJournalWriter;
  private long mCounter;

  public FlushCounter(AsyncJournalWriter asyncJournalWriter) {
    mAsyncJournalWriter = asyncJournalWriter;
    mCounter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
  }

  public void setCounter(long counter) {
    mCounter = counter;
  }

  @Override
  public void close() throws IOException {
    if (mCounter == AsyncJournalWriter.INVALID_FLUSH_COUNTER) {
      // Check this before the precondition.
      return;
    }
    Preconditions.checkNotNull(mAsyncJournalWriter, PreconditionMessage.ASYNC_JOURNAL_WRITER_NULL);

    RetryPolicy retry = new TimeoutRetry(JOURNAL_FLUSH_RETRY_TIMEOUT_MS, Constants.SECOND_MS);
    int attempts = 0;
    while (retry.attemptRetry()) {
      try {
        attempts++;
        mAsyncJournalWriter.flush(mCounter);
        return;
      } catch (IOException e) {
        LOG.warn("Journal flush failed. retrying...", e);
      }
    }
    LOG.error(
        "Journal flush failed after {} attempts. Terminating process to prevent inconsistency.",
        attempts);
    if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
      throw new RuntimeException("Journal flush failed after " + attempts
          + " attempts. Terminating process to prevent inconsistency.");
    }
    System.exit(-1);
  }
}
