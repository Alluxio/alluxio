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

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import tachyon.Constants;

/**
 * A list of non-empty {@link TTLBucket}s sorted by ttl interval start time of each bucket.
 * <p/>
 * Two adjacent buckets may not have adjacent intervals since there may be no files with ttl value
 * in the skipped intervals.
 * <p/>
 * Thread-safety is guaranteed by {@link ConcurrentSkipListSet}.
 */
public final class TTLBucketList {
  /**
   * List of buckets sorted by interval start time. SkipList is used for O(logn) insertion and
   * retrieval, see {@link ConcurrentSkipListSet}.
   */
  private final ConcurrentSkipListSet<TTLBucket> mBucketList;

  public TTLBucketList() {
    mBucketList = new ConcurrentSkipListSet<TTLBucket>();
  }

  /**
   * Inserts an {@link InodeFile} to the appropriate bucket where its ttl end time lies in the
   * bucket's interval, if no appropriate bucket exists, a new bucket will be created to contain
   * this file, if ttl value is {@link Constants#NO_TTL}, the file won't be inserted to any buckets
   * and nothing will happen.
   *
   * @param file the file to be inserted
   */
  public void insert(InodeFile file) {
    if (file.getTTL() == Constants.NO_TTL) {
      return;
    }

    long ttlEndTimeMs = file.getCreationTimeMs() + file.getTTL();
    // Gets the last bucket with interval start time less than or equal to the file's life end
    // time.
    TTLBucket bucket = mBucketList.floor(new TTLBucket(ttlEndTimeMs));
    if (bucket == null || bucket.getTTLIntervalEndTimeMs() <= ttlEndTimeMs) {
      // 1. There is no bucket in the list, or
      // 2. All buckets' interval start time is larger than the file's life end time, or
      // 3. No bucket actually contains ttlEndTimeMs in its interval.
      // So a new bucket should should be added with an appropriate interval start. Assume the list
      // of buckets have continuous intervals, and the first interval starts at 0, then ttlEndTimeMs
      // should be in number (ttlEndTimeMs / interval) interval, so the start time of this interval
      // should be (ttlEndTimeMs / interval) * interval.
      long interval = TTLBucket.getTTLIntervalMs();
      bucket = new TTLBucket(ttlEndTimeMs / interval * interval);
      mBucketList.add(bucket);
    }
    bucket.addFile(file);
  }

  /**
   * Retrieves buckets whose ttl interval has expired before the specified time, that is, the
   * bucket's interval start time should be less than or equal to (specified time - ttl interval).
   * The returned set is backed by the internal set.
   *
   * @param time the expiration time
   * @return a set of expired buckets or an empty set if no buckets have expired
   */
  public Set<TTLBucket> getExpiredBuckets(long time) {
    return mBucketList.headSet(new TTLBucket(time - TTLBucket.getTTLIntervalMs()), true);
  }

  /**
   * Remove all buckets in the set.
   *
   * @param buckets a set of buckets to be removed
   */
  public void removeBuckets(Set<TTLBucket> buckets) {
    mBucketList.removeAll(buckets);
  }
}
