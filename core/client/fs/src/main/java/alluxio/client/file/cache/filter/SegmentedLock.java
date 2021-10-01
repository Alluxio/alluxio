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

package alluxio.client.file.cache.filter;

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
   * Non-exclusively acquires the lock of the bucket, blocking if necessary until available.
   *
   * @param b the bucket to be locked
   */
  public void lockOneRead(int b) {
    int i = getSegmentIndex(b);
    mLocks[i].readLock();
  }

  /**
   * Releases the read lock of the bucket if it is held.
   *
   * @param b the bucket to be unlocked
   */
  public void unlockOneRead(int b) {
    int i = getSegmentIndex(b);
    mLocks[i].tryUnlockRead();
  }

  /**
   * Exclusively acquires the lock of the bucket, blocking if necessary until available.
   *
   * @param b the bucket to be locked
   */
  public void lockOneWrite(int b) {
    int i = getSegmentIndex(b);
    mLocks[i].writeLock();
  }

  /**
   * Releases the write lock of bucket if it is held.
   *
   * @param b the bucket to be unlocked
   */
  public void unlockOneWrite(int b) {
    int i = getSegmentIndex(b);
    mLocks[i].tryUnlockWrite();
  }

  /**
   * Non-exclusively acquires the locks of two buckets, blocking if necessary until available.
   *
   * @param b1 the first bucket to be locked
   * @param b2 the second bucket to be locked
   */
  public void lockTwoRead(int b1, int b2) {
    int i1 = getSegmentIndex(b1);
    int i2 = getSegmentIndex(b2);
    if (i1 > i2) {
      int tmp = i1;
      i1 = i2;
      i2 = tmp;
    }
    mLocks[i1].readLock();
    if (i2 != i1) {
      mLocks[i2].readLock();
    }
  }

  /**
   * Releases the read locks of two buckets if they are held.
   *
   * @param b1 the first bucket to be unlocked
   * @param b2 the second bucket to be unlocked
   */
  public void unlockTwoRead(int b1, int b2) {
    int i1 = getSegmentIndex(b1);
    int i2 = getSegmentIndex(b2);
    // Question: is unlock order important ?
    mLocks[i1].tryUnlockRead();
    mLocks[i2].tryUnlockRead();
  }

  /**
   * Exclusively acquires the locks of two buckets, blocking if necessary until available.
   *
   * @param b1 the first bucket to be locked
   * @param b2 the second bucket to be locked
   */
  public void lockTwoWrite(int b1, int b2) {
    int i1 = getSegmentIndex(b1);
    int i2 = getSegmentIndex(b2);
    if (i1 > i2) {
      int tmp = i1;
      i1 = i2;
      i2 = tmp;
    }
    mLocks[i1].writeLock();
    if (i2 != i1) {
      mLocks[i2].writeLock();
    }
  }

  /**
   * Releases the write locks of two buckets if they are held.
   *
   * @param b1 the first bucket to be unlocked
   * @param b2 the second bucket to be unlocked
   */
  public void unlockTwoWrite(int b1, int b2) {
    int i1 = getSegmentIndex(b1);
    int i2 = getSegmentIndex(b2);
    // Question: is unlock order important ?
    mLocks[i1].tryUnlockWrite();
    mLocks[i2].tryUnlockWrite();
  }

  /**
   * Exclusively acquires the locks of three buckets, blocking if necessary until available.
   *
   * @param b1 the first bucket to be locked
   * @param b2 the second bucket to be locked
   * @param b3 the third bucket to be locked
   */
  public void lockThreeWrite(int b1, int b2, int b3) {
    int i1 = getSegmentIndex(b1);
    int i2 = getSegmentIndex(b2);
    int i3 = getSegmentIndex(b3);
    int tmp;
    if (i1 > i2) {
      tmp = i1;
      i1 = i2;
      i2 = tmp;
    }
    if (i2 > i3) {
      tmp = i2;
      i2 = i3;
      i3 = tmp;
    }
    if (i1 > i2) {
      tmp = i1;
      i1 = i2;
      i2 = tmp;
    }
    mLocks[i1].writeLock();
    if (i2 != i1) {
      mLocks[i2].writeLock();
    }
    if (i3 != i2) {
      mLocks[i3].writeLock();
    }
  }

  /**
   * Exclusively acquires the lock of ith segment, blocking if necessary until available.
   *
   * @param i the segment to be locked
   */
  public void lockOneSegmentWrite(int i) {
    mLocks[i].writeLock();
  }

  /**
   * Releases the write lock of ith segment if it is held.
   *
   * @param i the segment to be unlocked
   */
  public void unlockOneSegmentWrite(int i) {
    mLocks[i].tryUnlockWrite();
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
   * @param i the segment
   * @return the start index of ith segment
   */
  public int getSegmentStartPos(int i) {
    return i << mMaskBits;
  }

  /**
   * @param b the bucket
   * @return the segment index which bucket b belongs to
   */
  public int getSegmentIndex(int b) {
    return b >> mMaskBits;
  }
}
