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
   * Non-exclusively acquires the lock of the bucket, blocking if necessary until available.
   *
   * @param b the bucket to be locked
   */
  public void readLock(int b) {
    int i = getSegmentIndex(b);
    mLocks[i].readLock();
  }

  /**
   * Non-exclusively acquires the locks of two buckets, blocking if necessary until available.
   *
   * @param b1 the first bucket to be locked
   * @param b2 the second bucket to be locked
   */
  public void readLock(int b1, int b2) {
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
   * Releases the read lock of the bucket if it is held.
   *
   * @param b the bucket to be unlocked
   */
  public void unlockRead(int b) {
    int i = getSegmentIndex(b);
    mLocks[i].tryUnlockRead();
  }

  /**
   * Releases the read locks of two buckets if they are held.
   *
   * @param b1 the first bucket to be unlocked
   * @param b2 the second bucket to be unlocked
   */
  public void unlockRead(int b1, int b2) {
    int i1 = getSegmentIndex(b1);
    int i2 = getSegmentIndex(b2);
    // Unlock order will no cause deadlock here as discussed in
    // https://stackoverflow.com/questions/1951275/would-you-explain-lock-ordering, but it's better
    // to unlock in reverse order to lock order.
    if (i1 > i2) {
      int tmp = i1;
      i1 = i2;
      i2 = tmp;
    }
    mLocks[i2].tryUnlockRead();
    if (i1 != i2) {
      mLocks[i1].tryUnlockRead();
    }
  }

  /**
   * Exclusively acquires the lock of the bucket, blocking if necessary until available.
   *
   * @param b the bucket to be locked
   */
  public void writeLock(int b) {
    int i = getSegmentIndex(b);
    mLocks[i].writeLock();
  }

  /**
   * Exclusively acquires the locks of two buckets, blocking if necessary until available.
   *
   * @param b1 the first bucket to be locked
   * @param b2 the second bucket to be locked
   */
  public void writeLock(int b1, int b2) {
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
   * Exclusively acquires the locks of three buckets, blocking if necessary until available.
   *
   * @param b1 the first bucket to be locked
   * @param b2 the second bucket to be locked
   * @param b3 the third bucket to be locked
   */
  public void writeLock(int b1, int b2, int b3) {
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
   * Releases the write lock of bucket if it is held.
   *
   * @param b the bucket to be unlocked
   */
  public void unlockWrite(int b) {
    int i = getSegmentIndex(b);
    mLocks[i].tryUnlockWrite();
  }

  /**
   * Releases the write locks of two buckets if they are held.
   *
   * @param b1 the first bucket to be unlocked
   * @param b2 the second bucket to be unlocked
   */
  public void unlockWrite(int b1, int b2) {
    int i1 = getSegmentIndex(b1);
    int i2 = getSegmentIndex(b2);
    if (i1 > i2) {
      int tmp = i1;
      i1 = i2;
      i2 = tmp;
    }
    mLocks[i2].tryUnlockWrite();
    if (i1 != i2) {
      mLocks[i1].tryUnlockWrite();
    }
  }

  /**
   * Releases the write locks of three buckets if they are held.
   *
   * @param b1 the first bucket to be unlocked
   * @param b2 the second bucket to be unlocked
   * @param b3 the third bucket to be unlocked
   */
  public void unlockWrite(int b1, int b2, int b3) {
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
    mLocks[i3].tryUnlockWrite();
    if (i2 != i3) {
      mLocks[i2].tryUnlockWrite();
    }
    if (i1 != i2) {
      mLocks[i1].tryUnlockWrite();
    }
  }

  /**
   * Exclusively acquires the lock of ith segment, blocking if necessary until available.
   *
   * @param i the segment to be locked
   */
  public void writeLockSegment(int i) {
    mLocks[i].writeLock();
  }

  /**
   * Releases the write lock of ith segment if it is held.
   *
   * @param i the segment to be unlocked
   */
  public void unlockWriteSegment(int i) {
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
