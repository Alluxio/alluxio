/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.file.meta;

import java.util.HashSet;
import java.util.Set;

import tachyon.Constants;
import tachyon.master.MasterContext;

/**
 * A bucket with all files whose ttl value lies in the bucket's time interval. The bucket's time
 * interval starts at a specific time and lasts for {@link Constants#MASTER_TTLCHECKER_INTERVAL_MS}.
 *
 * Not thread-safe. Only for use related to {@link TTLBucketList}.
 */
public final class TTLBucket implements Comparable<TTLBucket> {
  /** The time interval of this bucket is the same as ttl checker's interval. Be at least 1. */
  private static final int TTL_INTERVAL_MS = Math.max(MasterContext.getConf().getInt(
      Constants.MASTER_TTLCHECKER_INTERVAL_MS), 1);
  /**
   * Each bucket has a time to live interval, this value is the start of the interval, interval
   * value is the same as the configuration of {@link Constants#MASTER_TTLCHECKER_INTERVAL_MS}.
   */
  private long mTTLIntervalStartTimeMs;
  /** A set of InodeFiles whose ttl value is in the range of this bucket's interval. */
  private Set<InodeFile> mFiles;

  public TTLBucket(long startTimeMs) {
    mTTLIntervalStartTimeMs = startTimeMs;
    mFiles = new HashSet<InodeFile>();
  }

  public long getTTLIntervalStartTimeMs() {
    return mTTLIntervalStartTimeMs;
  }

  public long getTTLIntervalEndTimeMs() {
    return mTTLIntervalStartTimeMs + TTL_INTERVAL_MS;
  }

  public static long getTTLIntervalMs() {
    return TTL_INTERVAL_MS;
  }

  /**
   * @return the set of all files in the bucket backed by the internal set, changes made to the
   *         returned set will be shown in the internal set, and vice versa
   */
  public Set<InodeFile> getFiles() {
    return mFiles;
  }

  /**
   * Adds a file to the bucket.
   *
   * @param file the file to be added
   */
  public void addFile(InodeFile file) {
    mFiles.add(file);
  }

  /**
   * Removes a file from the bucket.
   *
   * @param file the file to be removed
   */
  public void removeFile(InodeFile file) {
    mFiles.remove(file);
  }

  /**
   * Compares this bucket's TTL interval start time to that of another bucket.
   *
   * @param ttlBucket the bucket to be compared to
   * @return 0 when return values of {@link #getTTLIntervalStartTimeMs()} from the two buckets are
   *         the same, -1 when that value of current instance is smaller, otherwise, 1
   */
  @Override
  public int compareTo(TTLBucket ttlBucket) {
    long startTime1 = getTTLIntervalStartTimeMs();
    long startTime2 = ttlBucket.getTTLIntervalStartTimeMs();
    if (startTime1 < startTime2) {
      return -1;
    }
    if (startTime1 == startTime2) {
      return 0;
    }
    return 1;
  }
}
