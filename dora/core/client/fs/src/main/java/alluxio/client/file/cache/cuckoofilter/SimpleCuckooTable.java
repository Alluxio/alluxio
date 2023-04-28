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

import alluxio.collections.BitSet;

import com.google.common.base.Preconditions;

import java.util.concurrent.ThreadLocalRandom;

/**
 * A simple cuckoo table.
 */
public class SimpleCuckooTable implements CuckooTable {
  private final int mTagsPerBucket;
  private final int mBitsPerTag;
  private final BitSet mBits;
  private final int mNumBuckets;

  /**
   * Create a single cuckoo table on given bit set.
   *
   * @param bitSet the bit set will be used as the underlying storage
   * @param numBuckets the number of buckets this table has
   * @param tagsPerBucket the number of slots each bucket has
   * @param bitsPerTag the number of bits each slot has
   */
  public SimpleCuckooTable(BitSet bitSet, int numBuckets, int tagsPerBucket, int bitsPerTag) {
    Preconditions.checkArgument(bitSet.size() == numBuckets * tagsPerBucket * bitsPerTag);
    mBits = bitSet;
    mNumBuckets = numBuckets;
    mTagsPerBucket = tagsPerBucket;
    mBitsPerTag = bitsPerTag;
  }

  @Override
  public int readTag(int bucketIndex, int slotIndex) {
    int tagStartIdx = getTagOffset(bucketIndex, slotIndex);
    int tag = 0;
    // TODO(iluoeli): Optimize me, since per bit operation is inefficient
    for (int k = 0; k < mBitsPerTag; k++) {
      // set corresponding bit in tag
      if (mBits.get(tagStartIdx + k)) {
        tag |= (1 << k);
      }
    }
    return tag;
  }

  @Override
  public void writeTag(int bucketIndex, int slotIndex, int tag) {
    int tagStartIdx = getTagOffset(bucketIndex, slotIndex);
    for (int k = 0; k < mBitsPerTag; k++) {
      if ((tag & (1L << k)) != 0) {
        mBits.set(tagStartIdx + k);
      } else {
        mBits.clear(tagStartIdx + k);
      }
    }
  }

  @Override
  public TagPosition findTag(int bucketIndex, int tag) {
    for (int slotIndex = 0; slotIndex < mTagsPerBucket; slotIndex++) {
      if (readTag(bucketIndex, slotIndex) == tag) {
        return new TagPosition(bucketIndex, slotIndex, CuckooStatus.OK);
      }
    }
    return new TagPosition(-1, -1, CuckooStatus.FAILURE_KEY_NOT_FOUND);
  }

  @Override
  public TagPosition findTag(int bucketIndex1, int bucketIndex2, int tag) {
    for (int slotIndex = 0; slotIndex < mTagsPerBucket; slotIndex++) {
      if (readTag(bucketIndex1, slotIndex) == tag) {
        return new TagPosition(bucketIndex1, slotIndex, CuckooStatus.OK);
      } else if (readTag(bucketIndex2, slotIndex) == tag) {
        return new TagPosition(bucketIndex2, slotIndex, CuckooStatus.OK);
      }
    }
    return new TagPosition(-1, -1, CuckooStatus.FAILURE_KEY_NOT_FOUND);
  }

  @Override
  public TagPosition deleteTag(int bucketIndex, int tag) {
    for (int slotIndex = 0; slotIndex < mTagsPerBucket; slotIndex++) {
      if (readTag(bucketIndex, slotIndex) == tag) {
        writeTag(bucketIndex, slotIndex, 0);
        return new TagPosition(bucketIndex, slotIndex, CuckooStatus.OK);
      }
    }
    return new TagPosition(-1, -1, CuckooStatus.FAILURE_KEY_NOT_FOUND);
  }

  @Override
  public int insertOrKickTag(int bucketIndex, int tag) {
    for (int slotIndex = 0; slotIndex < mTagsPerBucket; slotIndex++) {
      if (readTag(bucketIndex, slotIndex) == 0) {
        writeTag(bucketIndex, slotIndex, tag);
        return 0;
      }
    }
    int r = ThreadLocalRandom.current().nextInt(mTagsPerBucket);
    int oldTag = readTag(bucketIndex, r);
    writeTag(bucketIndex, r, tag);
    return oldTag;
  }

  @Override
  public int getNumTagsPerBuckets() {
    return mTagsPerBucket;
  }

  @Override
  public int getNumBuckets() {
    return mNumBuckets;
  }

  @Override
  public int getBitsPerTag() {
    return mBitsPerTag;
  }

  @Override
  public int getSizeInBytes() {
    return this.mBits.size() >> 3;
  }

  @Override
  public int getSizeInTags() {
    return this.mNumBuckets * mTagsPerBucket;
  }

  /**
   * @param bucketIndex the bucket index
   * @param posInBucket the slot
   * @return the start index of tag in bit set for given position
   */
  private int getTagOffset(int bucketIndex, int posInBucket) {
    return (bucketIndex * mTagsPerBucket * mBitsPerTag) + (posInBucket * mBitsPerTag);
  }
}
