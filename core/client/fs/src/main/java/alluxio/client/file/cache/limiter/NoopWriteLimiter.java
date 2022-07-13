package alluxio.client.file.cache.limiter;

import alluxio.conf.AlluxioConfiguration;

/**
 * Noop write limiter.
 */
public class NoopWriteLimiter implements WriteLimiter {
  /**
   * Constructor for NoopWriteLimiter.
   * @param conf
   */
  public NoopWriteLimiter(AlluxioConfiguration conf) {
  }

  @Override
  public boolean tryAcquire(int writeLength) {
    return true;
  }
}
