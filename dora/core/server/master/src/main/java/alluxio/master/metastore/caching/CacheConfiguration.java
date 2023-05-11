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

package alluxio.master.metastore.caching;

/**
 * Cache configuration.
 */
public class CacheConfiguration {
  private final int mMaxSize;
  private final int mHighWaterMark;
  private final int mLowWaterMark;
  private final int mEvictBatchSize;

  private CacheConfiguration(int maxSize, int highWaterMark, int lowWaterMark, int evictBatchSize) {
    mMaxSize = maxSize;
    mHighWaterMark = highWaterMark;
    mLowWaterMark = lowWaterMark;
    mEvictBatchSize = evictBatchSize;
  }

  /**
   * @return the max size
   */
  public int getMaxSize() {
    return mMaxSize;
  }

  /**
   * @return the high water mark
   */
  public int getHighWaterMark() {
    return mHighWaterMark;
  }

  /**
   * @return the low water mark
   */
  public int getLowWaterMark() {
    return mLowWaterMark;
  }

  /**
   * @return the eviction batch size
   */
  public int getEvictBatchSize() {
    return mEvictBatchSize;
  }

  /**
   * @return a cache configuration builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Cache configuration builder.
   */
  public static class Builder {
    private int mMaxSize;
    private int mHighWaterMark;
    private int mLowWaterMark;
    private int mEvictBatchSize;

    /**
     * @param maxSize the target max cache size
     * @return the builder
     */
    public Builder setMaxSize(int maxSize) {
      mMaxSize = maxSize;
      return this;
    }

    /**
     * @param highWaterMark the high water mark where the cache should begin evicting
     * @return the builder
     */
    public Builder setHighWaterMark(int highWaterMark) {
      mHighWaterMark = highWaterMark;
      return this;
    }

    /**
     * @param lowWaterMark the low water mark where the cache should evict down to
     * @return the builder
     */
    public Builder setLowWaterMark(int lowWaterMark) {
      mLowWaterMark = lowWaterMark;
      return this;
    }

    /**
     * @param evictBatchSize the batch size for cache eviction
     * @return the builder
     */
    public Builder setEvictBatchSize(int evictBatchSize) {
      mEvictBatchSize = evictBatchSize;
      return this;
    }

    /**
     * @return a cache configuration based on the values passed to the builder
     */
    public CacheConfiguration build() {
      return new CacheConfiguration(mMaxSize, mHighWaterMark, mLowWaterMark, mEvictBatchSize);
    }
  }
}
