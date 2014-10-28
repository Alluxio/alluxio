package tachyon.retry;

/**
 * Attempts to retry code from a do/while loop. The way that this interface works is that the logic
 * for delayed retries (retries that sleep) can delay the caller of {@link #attemptRetry()}. Because
 * of this, its best to put retries in do/while loops to avoid the first wait.
 * <p />
 * This interface is not thread safe and as such should only ever be used from one thread.
 */
public interface RetryPolicy {

  /**
   * How many retries have been performed. If no retries have been performed, 0 is returned.
   */
  int getRetryCount();

  /**
   * Attempts to run the given operation, returning false if unable to (max retries have happened).
   */
  boolean attemptRetry();
}
