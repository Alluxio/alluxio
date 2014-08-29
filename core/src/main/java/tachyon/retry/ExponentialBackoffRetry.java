package tachyon.retry;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

public final class ExponentialBackoffRetry implements RetryPolicy {
  private final Random mRandom = new Random();
  private final int mBaseSleepTimeMs;
  private final int mMaxSleepMs;
  private final int mMaxRetries;
  private int mCount = 0;

  public ExponentialBackoffRetry(int baseSleepTimeMs, int maxSleepMs, int maxRetries) {
    Preconditions.checkArgument(baseSleepTimeMs >= 0, "Base must be a positive number, or 0");
    Preconditions.checkArgument(maxSleepMs >= 0, "Max must be a positive number, or 0");
    Preconditions.checkArgument(maxRetries > 0, "Max retries must be a positive number");

    mBaseSleepTimeMs = baseSleepTimeMs;
    mMaxSleepMs = maxSleepMs;
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

  private long getSleepTime() {
    // copied from Hadoop's RetryPolicies.java
    int sleepMs = mBaseSleepTimeMs * Math.max(1, mRandom.nextInt(1 << (mCount + 1)));
    if (sleepMs > mMaxSleepMs) {
      sleepMs = mMaxSleepMs;
    }
    return sleepMs;
  }
}
