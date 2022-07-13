package alluxio.client.file.cache.limiter;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;

/**
 * Interface of write limiter for ssd endurance.
 */
public interface WriteLimiter {

  /**
   * Create a WriteLimiter.
   * @param conf the alluxio configuration
   * @return the write limiter
   */
  static WriteLimiter create(AlluxioConfiguration conf) {
    return CommonUtils.createNewClassInstance(
        conf.getClass(PropertyKey.USER_CLIENT_CACHE_WRITE_LIMITER_CLASS),
        new Class[] {AlluxioConfiguration.class},
        new Object[] {conf});
  }

  /**
   * @param writeLength
   * @return Whether to allow writing
   */
  boolean tryAcquire(int writeLength);
}
