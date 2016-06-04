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

import alluxio.Constants;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A list of non-empty {@link TtlBucket}s sorted by ttl interval start time of each bucket.
 * <p>
 * Two adjacent buckets may not have adjacent intervals since there may be no files with ttl value
 * in the skipped intervals.
 */
@ThreadSafe
public final class TtlBucketList {
  /**
   * List of buckets sorted by interval start time. SkipList is used for O(logn) insertion and
   * retrieval, see {@link ConcurrentSkipListSet}.
   */
  private final ConcurrentSkipListSet<TtlBucket> mBucketList;

  /**
   * Creates a new list of {@link TtlBucket}s.
   */
  public TtlBucketList() {
    mBucketList = new ConcurrentSkipListSet<>();
  }

  /**
   * Gets the bucket in the list that contains the file.
   *
   * @param file the file to be contained
   * @return the bucket containing the file, or null if no such bucket exists
   */
  private TtlBucket getBucketContaining(InodeFile file) {
    if (file.getTtl() == Constants.NO_TTL) {
      // no bucket will contain a file with NO_TTL.
      return null;
    }

    long ttlEndTimeMs = file.getCreationTimeMs() + file.getTtl();
    // Gets the last bucket with interval start time less than or equal to the file's life end
    // time.
    TtlBucket bucket = mBucketList.floor(new TtlBucket(ttlEndTimeMs));
    if (bucket == null || bucket.getTtlIntervalEndTimeMs() < ttlEndTimeMs
        || (bucket.getTtlIntervalEndTimeMs() == ttlEndTimeMs
            && TtlBucket.getTtlIntervalMs() != 0)) {
      // 1. There is no bucket in the list, or
      // 2. All buckets' interval start time is larger than the file's life end time, or
      // 3. No bucket actually contains ttlEndTimeMs in its interval.
      return null;
    }

    return bucket;
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
    if (file.getTtl() == Constants.NO_TTL) {
      return;
    }

    TtlBucket bucket = getBucketContaining(file);
    if (bucket == null) {
      long ttlEndTimeMs = file.getCreationTimeMs() + file.getTtl();
      // No bucket contains the file, so a new bucket should be added with an appropriate interval
      // start. Assume the list of buckets have continuous intervals, and the first interval starts
      // at 0, then ttlEndTimeMs should be in number (ttlEndTimeMs / interval) interval, so the
      // start time of this interval should be (ttlEndTimeMs / interval) * interval.
      long interval = TtlBucket.getTtlIntervalMs();
      bucket = new TtlBucket(interval == 0 ? ttlEndTimeMs : ttlEndTimeMs / interval * interval);
      mBucketList.add(bucket);
    }
    bucket.addFile(file);
  }

  /**
   * Removes a file from the bucket containing it if the file is in one of the buckets, otherwise,
   * do nothing.
   *
   * <p>
   * Assume that no file in the buckets has ttl value that equals {@link Constants#NO_TTL}.
   * If a file with valid ttl value is inserted to the buckets and its ttl value is going to be set
   * to {@link Constants#NO_TTL} later, be sure to remove the file from the buckets first.
   *
   * @param file the file to be removed
   */
  public void remove(InodeFile file) {
    TtlBucket bucket = getBucketContaining(file);
    if (bucket != null) {
      bucket.removeFile(file);
    }
  }

  /**
   * Retrieves buckets whose ttl interval has expired before the specified time, that is, the
   * bucket's interval start time should be less than or equal to (specified time - ttl interval).
   * The returned set is backed by the internal set.
   *
   * @param time the expiration time
   * @return a set of expired buckets or an empty set if no buckets have expired
   */
  public Set<TtlBucket> getExpiredBuckets(long time) {
    return mBucketList.headSet(new TtlBucket(time - TtlBucket.getTtlIntervalMs()), true);
  }

  /**
   * Removes all buckets in the set.
   *
   * @param buckets a set of buckets to be removed
   */
  public void removeBuckets(Set<TtlBucket> buckets) {
    mBucketList.removeAll(buckets);
  }
}
