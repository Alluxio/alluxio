/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache.filter;

import java.util.concurrent.locks.StampedLock;

public class SegmentedLock {
  private final int numLocks;
  private final int bitsOfLock;
  private final int numBuckets;
  private final int maskBits;
  private final StampedLock[] locks;
  private final int numBucketsPerSegment;

  public SegmentedLock(int numLocks, int numBuckets) {
    int highestBit = Integer.highestOneBit(numLocks);
    if (highestBit < numLocks) {
      numLocks = highestBit << 1;
    }
    this.numLocks = numLocks;
    this.bitsOfLock = Integer.numberOfTrailingZeros(numLocks);
    this.numBuckets = numBuckets;
    this.numBucketsPerSegment = numBuckets / numLocks;
    int bitsOfBuckets = Integer.numberOfTrailingZeros(Integer.highestOneBit(numBuckets));
    this.maskBits = bitsOfBuckets - bitsOfLock;
    locks = new StampedLock[numLocks];
    for (int i = 0; i < numLocks; i++) {
      locks[i] = new StampedLock();
    }
  }

  public void lockOneRead(int b) {
    int i = getSegmentIndex(b);
    locks[i].readLock();
  }

  public void unlockOneRead(int b) {
    int i = getSegmentIndex(b);
    locks[i].tryUnlockRead();
  }

  public void lockOneWrite(int b) {
    int i = getSegmentIndex(b);
    locks[i].writeLock();
  }

  public void unlockOneWrite(int b) {
    int i = getSegmentIndex(b);
    locks[i].tryUnlockWrite();
  }

  public void lockTwoRead(int b1, int b2) {
    int i1 = getSegmentIndex(b1);
    int i2 = getSegmentIndex(b2);
    if (i1 > i2) {
      int tmp = i1;
      i1 = i2;
      i2 = tmp;
    }
    locks[i1].readLock();
    if (i2 != i1) {
      locks[i2].readLock();
    }
  }

  public void unlockTwoRead(int b1, int b2) {
    int i1 = getSegmentIndex(b1);
    int i2 = getSegmentIndex(b2);
    // Question: is unlock order important ?
    locks[i1].tryUnlockRead();
    locks[i2].tryUnlockRead();
  }

  public void lockTwoWrite(int b1, int b2) {
    int i1 = getSegmentIndex(b1);
    int i2 = getSegmentIndex(b2);
    if (i1 > i2) {
      int tmp = i1;
      i1 = i2;
      i2 = tmp;
    }
    locks[i1].writeLock();
    if (i2 != i1) {
      locks[i2].writeLock();
    }
  }

  public void unlockTwoWrite(int b1, int b2) {
    int i1 = getSegmentIndex(b1);
    int i2 = getSegmentIndex(b2);
    // Question: is unlock order important ?
    locks[i1].tryUnlockWrite();
    locks[i2].tryUnlockWrite();
  }

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
    locks[i1].writeLock();
    if (i2 != i1) {
      locks[i2].writeLock();
    }
    if (i3 != i2) {
      locks[i3].writeLock();
    }
  }

  /**
   * Lock ith segment.
   */
  public void lockOneSegmentWrite(int i) {
    locks[i].writeLock();
  }

  public void unlockOneSegmentWrite(int i) {
    locks[i].tryUnlockWrite();
  }

  public int getNumLocks() {
    return numLocks;
  }

  public int getNumBucketsPerSegment() {
    return numBucketsPerSegment;
  }

  /**
   * Get the start index of ith segment.
   */
  public int getSegmentStartPos(int i) {
    return i << maskBits;
  }

  /**
   * Get the segment index which bucket b belongs to.
   */
  public int getSegmentIndex(int b) {
    return b >> maskBits;
  }
}
