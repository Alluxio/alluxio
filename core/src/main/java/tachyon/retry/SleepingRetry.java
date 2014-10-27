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

  @Override
  public int getRetryCount() {
    return mCount;
  }

  @Override
  public boolean attemptRetry() {
    if (mMaxRetries > mCount) {
      try {
        getSleepUnit().sleep(getSleepTime());
        mCount ++;
        return true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }

  /**
   * Unit of time that {@link #getSleepTime()} is measured in. Defaults to
   * {@link java.util.concurrent.TimeUnit#MILLISECONDS}.
   */
  protected TimeUnit getSleepUnit() {
    return TimeUnit.MILLISECONDS;
  }

  /**
   * How long to sleep before the next retry is performed. This method is used with
   * {@link #getSleepUnit()}, so all time given here must match the unit provided.
   */
  protected abstract long getSleepTime();
}
