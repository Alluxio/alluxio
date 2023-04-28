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

import java.util.concurrent.atomic.AtomicLong;

/**
 * A size encoder that supports encode/decode size information.
 */
public class SizeEncoder {
  protected final int mMaxSizeBits;
  protected final int mSizeGroupBits;
  protected final int mBitsPerBucket;
  protected final int mNumBuckets;
  protected final Bucket[] mBuckets;

  private class Bucket {
    private final AtomicLong mTotalBytes = new AtomicLong(0);
    private final AtomicLong mCount = new AtomicLong(0);

    public void add(int size) {
      mTotalBytes.addAndGet(size);
      mCount.incrementAndGet();
    }

    public int decrement() {
      int size = (int) getAverageSize();
      mTotalBytes.addAndGet(-size);
      mCount.decrementAndGet();
      return size;
    }

    public long getAverageSize() {
      return Math.round(mTotalBytes.get() / mCount.doubleValue());
    }

    public long getSize() {
      return mTotalBytes.get();
    }

    public long getCount() {
      return mCount.get();
    }
  }

  /**
   * Creates a new instance of {@link SizeEncoder}.
   * @param maxSizeBits the maximum size in bits
   * @param numBucketsBits the number of prefix bits of size
   */
  public SizeEncoder(int maxSizeBits, int numBucketsBits) {
    mMaxSizeBits = maxSizeBits;
    mSizeGroupBits = numBucketsBits;
    mBitsPerBucket = maxSizeBits - numBucketsBits;
    mNumBuckets = (1 << numBucketsBits);
    mBuckets = new Bucket[mNumBuckets];
    for (int i = 0; i < mNumBuckets; i++) {
      mBuckets[i] = new Bucket();
    }
  }

  /**
   * Adds a new size to the encoder.
   * @param size the size to add
   */
  public void add(int size) {
    mBuckets[getSizeGroup(size)].add(size);
  }

  /**
   * Decrements the size from the encoder.
   * @param group the group to decrement
   * @return the size of the bucket
   */
  public int dec(int group) {
    int size = mBuckets[group].decrement();
    return size;
  }

  private int getSizeGroup(int size) {
    return Math.min((size >> mBitsPerBucket), mNumBuckets - 1);
  }

  /**
   * Encode the size into a group.
   * @param size the size to encode
   * @return the group
   */
  public int encode(int size) {
    return getSizeGroup(size);
  }

  /**
   * Get info of buckets in the encoder.
   * @return the info of buckets
   */
  public String dumpInfo() {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < mNumBuckets; i++) {
      stringBuilder.append(String.format("%d [%d, %d] <%d, %d>%n",
          i, (1 << mBitsPerBucket) * i, (1 << mBitsPerBucket) * (i + 1),
          mBuckets[i].getCount(), mBuckets[i].getSize()));
    }
    return stringBuilder.toString();
  }
}

