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
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.master.journal.checkpoint.Checkpointed;
import alluxio.master.metastore.ReadOnlyInodeStore;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A list of non-empty {@link TtlBucket}s sorted by ttl interval start time of each bucket.
 * <p>
 * Two adjacent buckets may not have adjacent intervals since there may be no inodes with ttl value
 * in the skipped intervals.
 */
@ThreadSafe
public final class TtlBucketList implements Checkpointed {
  private static final Logger LOG = LoggerFactory.getLogger(TtlBucketList.class);

  /**
   * List of buckets sorted by interval start time. SkipList is used for O(logn) insertion and
   * retrieval, see {@link ConcurrentSkipListSet}.
   */
  private final ConcurrentSkipListSet<TtlBucket> mBucketList;
  private final ReadOnlyInodeStore mInodeStore;

  /**
   * Creates a new list of {@link TtlBucket}s.
   *
   * @param inodeStore the inode store
   */
  public TtlBucketList(ReadOnlyInodeStore inodeStore) {
    mInodeStore = inodeStore;
    mBucketList = new ConcurrentSkipListSet<>();
  }

  /**
   * Load inode from inode store on processing the provided inode id.
   * @param inodeId
   * @return Inode
   */
  public Inode loadInode(long inodeId) {
    return mInodeStore.get(inodeId).orElseGet(null);
  }

  /**
   * @return the number of TTL buckets
   */
  public int getNumBuckets() {
    return mBucketList.size();
  }

  /**
   * @return the total number of inodes in all the buckets
   */
  public long getNumInodes() {
    return mBucketList.stream().mapToInt((bucket) -> bucket.size()).sum();
  }

  /**
   * Gets the bucket in the list that contains the inode.
   *
   * @param inode the inode to be contained
   * @return the bucket containing the inode, or null if no such bucket exists
   */
  @Nullable
  private TtlBucket getBucketContaining(InodeView inode) {
    if (inode.getTtl() == Constants.NO_TTL) {
      // no bucket will contain a inode with NO_TTL.
      return null;
    }

    long ttlEndTimeMs = inode.getCreationTimeMs() + inode.getTtl();
    // Gets the last bucket with interval start time less than or equal to the inode's life end
    // time.
    TtlBucket bucket = mBucketList.floor(new TtlBucket(ttlEndTimeMs));
    if (bucket == null || bucket.getTtlIntervalEndTimeMs() < ttlEndTimeMs
        || (bucket.getTtlIntervalEndTimeMs() == ttlEndTimeMs
            && TtlBucket.getTtlIntervalMs() != 0)) {
      // 1. There is no bucket in the list, or
      // 2. All buckets' interval start time is larger than the inode's life end time, or
      // 3. No bucket actually contains ttlEndTimeMs in its interval.
      return null;
    }

    return bucket;
  }

  /**
   * Insert inode to the ttlbucket with default number of retry attempts.
   * @param inode
   */
  public void insert(Inode inode) {
    insert(inode, TtlBucket.DEFAULT_RETRY_ATTEMPTS);
  }

  /**
   * Inserts an inode to the appropriate bucket where its ttl end time lies in the
   * bucket's interval, if no appropriate bucket exists, a new bucket will be created to contain
   * this inode, if ttl value is {@link Constants#NO_TTL}, the inode won't be inserted to any
   * buckets and nothing will happen.
   *
   * @param inode the inode to be inserted
   * @param numOfRetry number of retries left to process this inode
   */
  public void insert(Inode inode, int numOfRetry) {
    if (inode.getTtl() == Constants.NO_TTL) {
      return;
    }

    TtlBucket bucket;
    while (true) {
      bucket = getBucketContaining(inode);
      if (bucket == null) {
        long ttlEndTimeMs = inode.getCreationTimeMs() + inode.getTtl();
        // No bucket contains the inode, so a new bucket should be added with an appropriate
        // interval start. Assume the list of buckets have continuous intervals, and the
        // first interval starts at 0, then ttlEndTimeMs should be in number
        // (ttlEndTimeMs / interval) interval, so the start time of this interval should be
        // (ttlEndTimeMs / interval) * interval.
        long interval = TtlBucket.getTtlIntervalMs();
        bucket = new TtlBucket(interval == 0 ? ttlEndTimeMs : ttlEndTimeMs / interval * interval);
        if (!mBucketList.add(bucket)) {
          // If we reach here, it means the same bucket has been concurrently inserted by another
          // thread, try again.
          continue;
        }
      }
      bucket.addInode(inode, numOfRetry);
      /* if we added to the bucket but it got concurrently polled by InodeTtlChecker,
      we're not sure this newly-added inode will be processed by the checker,
      so we need to try insert again. */
      if (mBucketList.contains(bucket)) {
        break;
      }
    }
  }

  /**
   * Removes an inode from the bucket containing it if the inode is in one
   * of the buckets, otherwise, do nothing.
   *
   * <p>
   * Assume that no inode in the buckets has ttl value that equals {@link Constants#NO_TTL}.
   * If an inode with valid ttl value is inserted to the buckets and its ttl value is
   * going to be set to {@link Constants#NO_TTL} later, be sure to remove the inode
   * from the buckets first.
   *
   * @param inode the inode to be removed
   */
  public void remove(InodeView inode) {
    TtlBucket bucket = getBucketContaining(inode);
    if (bucket != null) {
      bucket.removeInode(inode);
    }
  }

  /**
   * Polls buckets whose ttl interval has expired before the specified time, that is, the
   * bucket's interval start time should be less than or equal to (specified time - ttl interval).
   * if concurrently there are new inodes added to those polled buckets, we check if after the
   * moment it got added and the bucket got polled out, we're not sure if InodeTtlChecker will
   * process it sa part of this batch, it will create a new bucket and added there to retry.
   * Check {@link TtlBucketList#insert(Inode)}
   * @param time the expiration time
   * @return a set of expired buckets or an empty set if no buckets have expired
   */
  public Set<TtlBucket> pollExpiredBuckets(long time) {
    Set<TtlBucket> expiredBuckets = new HashSet<>();
    TtlBucket upperBound = new TtlBucket(time - TtlBucket.getTtlIntervalMs());
    while (!mBucketList.isEmpty() && mBucketList.first().compareTo(upperBound) <= 0) {
      expiredBuckets.add(mBucketList.pollFirst());
    }
    return expiredBuckets;
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.TTL_BUCKET_LIST;
  }

  /*
  Checkpointing a snapshot of the current inodes in ttlbucketlist. It's ok we checkpointed
  some inodes that have already been processed during the process as the expiry of inode
  will be double-checked at time of processing in InodeTtlChecker.
   */
  @Override
  public void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    CheckpointOutputStream cos = new CheckpointOutputStream(output, CheckpointType.LONGS);
    for (TtlBucket bucket : mBucketList) {
      for (long inodeId : bucket.getInodeIds()) {
        cos.writeLong(inodeId);
      }
    }
  }

  @Override
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    mBucketList.clear();
    Preconditions.checkState(input.getType() == CheckpointType.LONGS,
        "Unexpected checkpoint type: %s", input.getType());
    while (true) {
      try {
        long id = input.readLong();
        Optional<Inode> inode = mInodeStore.get(id);
        if (inode.isPresent()) {
          insert(inode.get());
        } else {
          LOG.error("Failed to find inode for id {}", id);
        }
      } catch (EOFException e) {
        break;
      }
    }
  }
}
