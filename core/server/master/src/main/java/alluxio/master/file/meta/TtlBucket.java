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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.collections.ConcurrentHashSet;

import com.google.common.base.Objects;

import java.util.Set;

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
  /**
   * Each bucket has a time to live interval, this value is the start of the interval, interval
   * value is the same as the configuration of {@link PropertyKey#MASTER_TTL_CHECKER_INTERVAL_MS}.
   */
  private final long mTtlIntervalStartTimeMs;
  /** A set of Inode whose ttl value is in the range of this bucket's interval. */
  private final ConcurrentHashSet<InodeView> mInodes;

  /**
   * Creates a new instance of {@link TtlBucket}.
   *
   * @param startTimeMs the start time to use
   */
  public TtlBucket(long startTimeMs) {
    mTtlIntervalStartTimeMs = startTimeMs;
    mInodes = new ConcurrentHashSet<>();
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
   * @return the set of all inodes in the bucket backed by the internal set, changes made to the
   *         returned set will be shown in the internal set, and vice versa
   */
  public Set<InodeView> getInodes() {
    return mInodes;
  }

  /**
   * Adds a inode to the bucket.
   *
   * @param inode the inode to be added
   */
  public void addInode(InodeView inode) {
    mInodes.add(inode);
  }

  /**
   * Removes a inode from the bucket.
   *
   * @param inode the inode to be removed
   */
  public void removeInode(InodeView inode) {
    mInodes.remove(inode);
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
