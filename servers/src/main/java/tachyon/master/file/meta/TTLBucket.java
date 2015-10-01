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

import java.util.LinkedList;
import java.util.List;

import tachyon.Constants;
import tachyon.master.MasterContext;

/**
 * A bucket with all files whose ttl value lies in the bucket's time interval. The bucket's time
 * interval starts at a specific time and lasts for {@link Constants#MASTER_TTLCHECKER_INTERVAL_MS}.
 *
 * Not thread-safe. Only for use related to {@link TTLBucketList}.
 */
public final class TTLBucket implements Comparable<TTLBucket> {
  /** The time interval of this bucket is the same as ttl checker's interval. */
  private static final int TTL_INTERVAL_MS = MasterContext.getConf().getInt(
      Constants.MASTER_TTLCHECKER_INTERVAL_MS);
  /**
   * Each bucket has a time to live interval, this value is the start of the interval, interval
   * value is the same as the configuration of {@link Constants#MASTER_TTLCHECKER_INTERVAL_MS}.
   */
  private long mTTLIntervalStartTimeMs;
  /** A list of InodeFiles whose ttl value is in the range of this bucket's interval. */
  private List<InodeFile> mFiles;

  public TTLBucket(long startTimeMs) {
    mTTLIntervalStartTimeMs = startTimeMs;
    mFiles = new LinkedList<InodeFile>();
  }

  public long getTTLIntervalStartTimeMs() {
    return mTTLIntervalStartTimeMs;
  }

  public void setTTLIntervalStartTimeMs(long startTimeMs) {
    mTTLIntervalStartTimeMs = startTimeMs;
  }

  public long getTTLIntervalEndTimeMs() {
    return mTTLIntervalStartTimeMs + TTL_INTERVAL_MS;
  }

  public static long getTTLIntervalMs() {
    return TTL_INTERVAL_MS;
  }

  public List<InodeFile> getFiles() {
    return mFiles;
  }

  public void addFile(InodeFile file) {
    mFiles.add(file);
  }

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
