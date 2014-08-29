package tachyon.retry;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

/**
 * A retry policy that uses thread sleeping for the delay.
 */
public abstract class SleepingRetry implements RetryPolicy {
  private final int mMaxRetries;
  private int mCount = 0;

  protected SleepingRetry(int maxRetries) {
    Preconditions.checkArgument(maxRetries > 0, "Max retries must be a positive number");
    mMaxRetries = maxRetries;
  }

  protected int getMaxRetries() {
    return mMaxRetries;
  }

  @Override
  public int getRetryCount() {
    return mCount;
  }

  @Override
  public boolean attemptRetry() {
    if (getMaxRetries() > mCount) {
      try {
        TimeUnit.MILLISECONDS.sleep(getSleepTime());
        mCount++;
        return true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }

  protected abstract long getSleepTime();
}
