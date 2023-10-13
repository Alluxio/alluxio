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

package alluxio.master.file.meta;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import com.google.common.base.Objects;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A bucket with all inodes whose ttl value lies in the bucket's time interval. The bucket's time
 * interval starts at a certain time and lasts for
 * {@link PropertyKey#MASTER_TTL_CHECKER_INTERVAL_MS}.
 */
@ThreadSafe
public final class TtlBucket implements Comparable<TtlBucket> {
  /**
   * The time interval of this bucket is the same as ttl checker's interval.
   *
   * This field is intentionally not final so that tests can change the value.
   */
  private static long sTtlIntervalMs =
      Configuration.getMs(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS);
  public static final int DEFAULT_RETRY_ATTEMPTS = 5;
  /**
   * Each bucket has a time to live interval, this value is the start of the interval, interval
   * value is the same as the configuration of {@link PropertyKey#MASTER_TTL_CHECKER_INTERVAL_MS}.
   */
  private final long mTtlIntervalStartTimeMs;
  /**
   * A collection containing those inodes whose ttl value is
   * in the range of this bucket's interval. The mapping
   * is from inode id to the number of left retry to process.
   */
  private final ConcurrentHashMap<Long, Integer> mInodeToRetryMap;

  /**
   * Creates a new instance of {@link TtlBucket}.
   *
   * @param startTimeMs the start time to use
   */
  public TtlBucket(long startTimeMs) {
    mTtlIntervalStartTimeMs = startTimeMs;
    mInodeToRetryMap = new ConcurrentHashMap<>();
  }

  /**
   * @return the ttl interval start time in milliseconds
   */
  public long getTtlIntervalStartTimeMs() {
    return mTtlIntervalStartTimeMs;
  }

  /**
   *
   * @return the ttl interval end time in milliseconds
   */
  public long getTtlIntervalEndTimeMs() {
    return mTtlIntervalStartTimeMs + sTtlIntervalMs;
  }

  /**
   * @return the ttl interval in milliseconds
   */
  public static long getTtlIntervalMs() {
    return sTtlIntervalMs;
  }

  /**
   * @return an unmodifiable view of all inodes ids in the bucket
   */
  public Collection<Long> getInodeIds() {
    return Collections.unmodifiableSet(mInodeToRetryMap.keySet());
  }

  /**
   * Get collection of inode to its left ttl process retry attempts.
   * @return collection of inode to its left ttl process retry attempts
   */
  public Collection<Map.Entry<Long, Integer>> getInodeExpiries() {
    return Collections.unmodifiableSet(mInodeToRetryMap.entrySet());
  }

  /**
   * Adds an inode with default num of retry attempt to expire.
   * @param inode
   */
  public void addInode(Inode inode) {
    addInode(inode, DEFAULT_RETRY_ATTEMPTS);
  }

  /**
   * Adds an inode to the bucket with a specific left retry number.
   *
   * @param inode the inode to be added
   * @param numOfRetry num of retries left when added to the ttlbucket
   */
  public void addInode(Inode inode, int numOfRetry) {
    mInodeToRetryMap.compute(inode.getId(), (k, v) -> {
      if (v != null) {
        return Math.min(v, numOfRetry);
      }
      return numOfRetry;
    });
  }

  /**
   * Removes an inode from the bucket.
   *
   * @param inode the inode to be removed
   */
  public void removeInode(InodeView inode) {
    mInodeToRetryMap.remove(inode.getId());
  }

  /**
   * @return the number of inodes in the bucket
   */
  public int size() {
    return mInodeToRetryMap.size();
  }

  /**
   * Compares this bucket's TTL interval start time to that of another bucket.
   *
   * @param ttlBucket the bucket to be compared to
   * @return 0 when return values of {@link #getTtlIntervalStartTimeMs()} from the two buckets are
   *         the same, -1 when that value of current instance is smaller, otherwise, 1
   */
  @Override
  public int compareTo(TtlBucket ttlBucket) {
    long startTime1 = getTtlIntervalStartTimeMs();
    long startTime2 = ttlBucket.getTtlIntervalStartTimeMs();
    return Long.compare(startTime1, startTime2);
  }

  /**
   * Compares to a specific object.
   *
   * @param o the object to compare
   * @return true if object is also {@link TtlBucket} and represents the same TtlIntervalStartTime
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TtlBucket)) {
      return false;
    }
    TtlBucket that = (TtlBucket) o;
    return mTtlIntervalStartTimeMs == that.mTtlIntervalStartTimeMs;
  }

  /**
   * Returns the hash code for the {@link TtlBucket}.
   *
   * @return The hash code value for this {@link TtlBucket}
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(mTtlIntervalStartTimeMs);
  }
}
