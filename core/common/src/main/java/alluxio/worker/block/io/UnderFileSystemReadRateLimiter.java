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

package alluxio.worker.block.io;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RateLimiter for reading ufs.
 */
public class UnderFileSystemReadRateLimiter {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemReadRateLimiter.class);

  private RateLimiter mReadLimiter;
  private long mReadBytes;
  private boolean mLimiterEnabled;

  /**
   * Constructs a new instance for {@link UnderFileSystemReadRateLimiter}.
   * @param permitsPerSecond  the rate of the RateLimiter
   */
  public UnderFileSystemReadRateLimiter(long permitsPerSecond) {
    mReadLimiter = RateLimiter.create(permitsPerSecond);
    mReadBytes = 0;
    mLimiterEnabled = false;
  }

  /**
   * Set the rate.
   *
   * @param permitsPerSecond new rate of the RateLimiter
   */
  public synchronized void setRate(long permitsPerSecond) {
    if (permitsPerSecond > 0) {
      if (!isLimiterEnabled()) {
        LOG.info("The throughput limit is enabled.");
      }
      mLimiterEnabled = true;
      mReadLimiter.setRate(permitsPerSecond);
    } else {
      if (isLimiterEnabled()) {
        LOG.warn("The throughput limit is disabled.");
      }
      mLimiterEnabled = false;
    }
    mReadBytes = 0;
  }

  /**
   * A blocked method to acquire resources.
   * @param permits num of requested resource
   * @return time spent sleeping to enforce rate
   */
  public synchronized double acquire(int permits) {
    if (isLimiterEnabled()) {
      mReadBytes += permits;
      return mReadLimiter.acquire(permits);
    }
    return 0;
  }

  /**
   * @return the stable rate
   */
  public synchronized long getRate() {
    return (long) mReadLimiter.getRate();
  }

  /**
   * @return ture if has read file
   */
  public synchronized boolean hasRead() {
    return mReadBytes != 0;
  }

  /**
   * @return true if limiter enabled
   */
  public synchronized boolean isLimiterEnabled() {
    return mLimiterEnabled;
  }
}
