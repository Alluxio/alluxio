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

package alluxio.client.file.cache.cuckoofilter;

import java.util.concurrent.locks.StampedLock;

/**
 * A segmented lock that supports methods to lock/unlock multiple buckets in order to avoid dead
 * lock.
 */
public class SegmentedLock {
  private final int mNumLocks;
  private final int mBitsOfLock;
  private final int mMaskBits;
  private final StampedLock[] mLocks;
  private final int mNumBucketsPerSegment;

  /**
   * Create a segmented lock with specified locks.
   *
   * @param numBuckets the number of buckets
   * @param numLocks the number of locks
   */
  public SegmentedLock(int numLocks, int numBuckets) {
    int highestBit = Integer.highestOneBit(numLocks);
    if (highestBit < numLocks) {
      numLocks = highestBit << 1;
    }
    mNumLocks = numLocks;
    mBitsOfLock = Integer.numberOfTrailingZeros(numLocks);
    mNumBucketsPerSegment = numBuckets / numLocks;
    int bitsOfBuckets = Integer.numberOfTrailingZeros(Integer.highestOneBit(numBuckets));
    mMaskBits = bitsOfBuckets - mBitsOfLock;
    mLocks = new StampedLock[numLocks];
    for (int i = 0; i < numLocks; i++) {
      mLocks[i] = new StampedLock();
    }
  }

  /**
   * Non-exclusively acquires the locks of two buckets, blocking if necessary until available.
   *
   * @param bucket1 the first bucket to be locked
   * @param bucket2 the second bucket to be locked
   */
  public void readLock(int bucket1, int bucket2) {
    int segmentIndex1 = getSegmentIndex(bucket1);
    int segmentIndex2 = getSegmentIndex(bucket2);
    mLocks[Math.min(segmentIndex1, segmentIndex2)].readLock();
    if (segmentIndex2 != segmentIndex1) {
      mLocks[Math.max(segmentIndex1, segmentIndex2)].readLock();
    }
  }

  /**
   * Releases the read locks of two buckets if they are held.
   *
   * @param bucket1 the first bucket to be unlocked
   * @param bucket2 the second bucket to be unlocked
   */
  public void unlockRead(int bucket1, int bucket2) {
    int segmentIndex1 = getSegmentIndex(bucket1);
    int segmentIndex2 = getSegmentIndex(bucket2);
    mLocks[Math.max(segmentIndex1, segmentIndex2)].tryUnlockRead();
    if (segmentIndex2 != segmentIndex1) {
      mLocks[Math.min(segmentIndex1, segmentIndex2)].tryUnlockRead();
    }
  }

  /**
   * Exclusively acquires the lock of the bucket, blocking if necessary until available.
   *
   * @param bucket the bucket to be locked
   */
  public void writeLock(int bucket) {
    mLocks[getSegmentIndex(bucket)].writeLock();
  }

  /**
   * Exclusively acquires the locks of two buckets, blocking if necessary until available.
   *
   * @param b1 the first bucket to be locked
   * @param b2 the second bucket to be locked
   */
  public void writeLock(int b1, int b2) {
    int segmentIndex1 = getSegmentIndex(b1);
    int segmentIndex2 = getSegmentIndex(b2);
    mLocks[Math.min(segmentIndex1, segmentIndex2)].writeLock();
    if (segmentIndex2 != segmentIndex1) {
      mLocks[Math.max(segmentIndex1, segmentIndex2)].writeLock();
    }
  }

  /**
   * Exclusively acquires the locks of three buckets, blocking if necessary until available.
   *
   * @param bucket1 the first bucket to be locked
   * @param bucket2 the second bucket to be locked
   * @param bucket3 the third bucket to be locked
   */
  public void writeLock(int bucket1, int bucket2, int bucket3) {
    int segmentIndex1 = getSegmentIndex(bucket1);
    int segmentIndex2 = getSegmentIndex(bucket2);
    int segmentIndex3 = getSegmentIndex(bucket3);
    int maxIndex = Math.max(segmentIndex1, Math.max(segmentIndex2, segmentIndex3));
    int minIndex = Math.min(segmentIndex1, Math.min(segmentIndex2, segmentIndex3));
    int midIndex = segmentIndex1 + segmentIndex2 + segmentIndex3 - maxIndex - minIndex;
    mLocks[minIndex].writeLock();
    if (midIndex != minIndex) {
      mLocks[midIndex].writeLock();
    }
    if (maxIndex != midIndex) {
      mLocks[maxIndex].writeLock();
    }
  }

  /**
   * Releases the write lock of bucket if it is held.
   *
   * @param bucket the bucket to be unlocked
   */
  public void unlockWrite(int bucket) {
    mLocks[getSegmentIndex(bucket)].tryUnlockWrite();
  }

  /**
   * Releases the write locks of two buckets if they are held.
   *
   * @param bucket1 the first bucket to be unlocked
   * @param bucket2 the second bucket to be unlocked
   */
  public void unlockWrite(int bucket1, int bucket2) {
    int segmentIndex1 = getSegmentIndex(bucket1);
    int segmentIndex2 = getSegmentIndex(bucket2);
    mLocks[Math.max(segmentIndex1, segmentIndex2)].tryUnlockWrite();
    if (segmentIndex2 != segmentIndex1) {
      mLocks[Math.min(segmentIndex1, segmentIndex2)].tryUnlockWrite();
    }
  }

  /**
   * Releases the write locks of three buckets if they are held.
   *
   * @param bucket1 the first bucket to be unlocked
   * @param bucket2 the second bucket to be unlocked
   * @param bucket3 the third bucket to be unlocked
   */
  public void unlockWrite(int bucket1, int bucket2, int bucket3) {
    int segmentIndex1 = getSegmentIndex(bucket1);
    int segmentIndex2 = getSegmentIndex(bucket2);
    int segmentIndex3 = getSegmentIndex(bucket3);
    int maxIndex = Math.max(segmentIndex1, Math.max(segmentIndex2, segmentIndex3));
    int minIndex = Math.min(segmentIndex1, Math.min(segmentIndex2, segmentIndex3));
    int midIndex = segmentIndex1 + segmentIndex2 + segmentIndex3 - maxIndex - minIndex;
    mLocks[maxIndex].tryUnlockWrite();
    if (midIndex != maxIndex) {
      mLocks[midIndex].tryUnlockWrite();
    }
    if (minIndex != midIndex) {
      mLocks[minIndex].tryUnlockWrite();
    }
  }

  /**
   * Exclusively acquires the lock of the ith segment, blocking if necessary until available.
   *
   * @param index of the segment to be locked
   */
  public void writeLockSegment(int index) {
    mLocks[index].writeLock();
  }

  /**
   * Releases the write lock of the ith segment if it is held.
   *
   * @param index of the segment to be unlocked
   */
  public void unlockWriteSegment(int index) {
    mLocks[index].tryUnlockWrite();
  }

  /**
   * @return the number of locks/segments
   */
  public int getNumLocks() {
    return mNumLocks;
  }

  /**
   * @return the number of buckets per segment guards
   */
  public int getNumBucketsPerSegment() {
    return mNumBucketsPerSegment;
  }

  /**
   * @param index index of the segment
   * @return the start index of ith segment
   */
  public int getSegmentStartPos(int index) {
    return index << mMaskBits;
  }

  /**
   * @param bucket the bucket
   * @return the segment index which the bucket belongs to
   */
  public int getSegmentIndex(int bucket) {
    return bucket >> mMaskBits;
  }
}
