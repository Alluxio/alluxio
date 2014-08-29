package tachyon.retry;

import java.util.Random;

import com.google.common.base.Preconditions;

public final class ExponentialBackoffRetry extends SleepingRetry {
  private final Random mRandom = new Random();
  private final int mBaseSleepTimeMs;
  private final int mMaxSleepMs;

  public ExponentialBackoffRetry(int baseSleepTimeMs, int maxSleepMs, int maxRetries) {
    super(maxRetries);
    Preconditions.checkArgument(baseSleepTimeMs >= 0, "Base must be a positive number, or 0");
    Preconditions.checkArgument(maxSleepMs >= 0, "Max must be a positive number, or 0");

    mBaseSleepTimeMs = baseSleepTimeMs;
    mMaxSleepMs = maxSleepMs;
  }

  @Override
  protected long getSleepTime() {
    // copied from Hadoop's RetryPolicies.java
    int sleepMs = mBaseSleepTimeMs * Math.max(1, mRandom.nextInt(1 << (getRetryCount() + 1)));
    if (sleepMs > mMaxSleepMs) {
      sleepMs = mMaxSleepMs;
    }
    return sleepMs;
  }
}
