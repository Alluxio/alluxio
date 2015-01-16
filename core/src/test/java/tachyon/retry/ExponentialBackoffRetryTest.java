package tachyon.retry;

import org.junit.Assert;
import org.junit.Test;

public class ExponentialBackoffRetryTest {

  @Test
  public void largeRetriesProducePositiveTime() {
    int max = 1000;
    MockExponentialBackoffRetry backoff =
        new MockExponentialBackoffRetry(50, Integer.MAX_VALUE, max);
    for (int i = 0; i < max; i ++) {
      backoff.setRetryCount(i);
      long time = backoff.getSleepTime();
      Assert.assertTrue("Time must always be positive: " + time, time > 0);
    }
  }

  public static final class MockExponentialBackoffRetry extends ExponentialBackoffRetry {
    private int retryCount = 0;

    public MockExponentialBackoffRetry(int baseSleepTimeMs, int maxSleepMs, int maxRetries) {
      super(baseSleepTimeMs, maxSleepMs, maxRetries);
    }

    @Override
    public int getRetryCount() {
      return retryCount;
    }

    public void setRetryCount(int retryCount) {
      this.retryCount = retryCount;
    }
  }
}
