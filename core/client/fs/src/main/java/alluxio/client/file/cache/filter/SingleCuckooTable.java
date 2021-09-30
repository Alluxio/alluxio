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

import java.util.concurrent.ThreadLocalRandom;

public class SingleCuckooTable implements CuckooTable {
  private final int mTagsPerBucket;
  private final int mBitsPerTag;
  private AbstractBitSet mBits;
  private int mNumBuckets;

  public SingleCuckooTable(AbstractBitSet bitSet, int numBuckets, int tagsPerBucket,
      int bitsPerTag) {
    this.mBits = bitSet;
    this.mNumBuckets = numBuckets;
    this.mTagsPerBucket = tagsPerBucket;
    this.mBitsPerTag = bitsPerTag;
    // TODO: check bits.size() and numBuckets*bitsPerTag
  }

  @Override
  public int readTag(int i, int j) {
    int tagStartIdx = getTagOffset(i, j);
    int tag = 0;
    // TODO(iluoeli): Optimize me, since per bit operation is inefficient
    for (int k = 0; k < mBitsPerTag; k++) {
      // set corresponding bit in tag
      int b = 0;
      if (mBits.get(tagStartIdx + k)) {
        b = 1;
        tag |= (b << k);
      }
    }
    return tag;
  }

  @Override
  public void writeTag(int i, int j, int t) {
    int tagStartIdx = getTagOffset(i, j);
    for (int k = 0; k < mBitsPerTag; k++) {
      if ((t & (1L << k)) != 0) {
        mBits.set(tagStartIdx + k);
      } else {
        mBits.clear(tagStartIdx + k);
      }
    }
  }

  @Override
  public boolean findTagInBucket(int i, int tag) {
    for (int j = 0; j < mTagsPerBucket; j++) {
      if (readTag(i, j) == tag) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean findTagInBuckets(int i1, int i2, int tag) {
    for (int j = 0; j < mTagsPerBucket; j++) {
      if (readTag(i1, j) == tag || readTag(i2, j) == tag) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean deleteTagFromBucket(int i, int tag) {
    for (int j = 0; j < mTagsPerBucket; j++) {
      if (readTag(i, j) == tag) {
        writeTag(i, j, 0);
        return true;
      }
    }
    return false;
  }

  @Override
  public int insertOrKickoutOne(int i, int tag) {
    for (int j = 0; j < mTagsPerBucket; j++) {
      if (readTag(i, j) == 0) {
        writeTag(i, j, tag);
        return 0;
      }
    }
    int r = ThreadLocalRandom.current().nextInt(mTagsPerBucket);
    int oldTag = readTag(i, r);
    writeTag(i, r, tag);
    return oldTag;
  }

  @Override
  public boolean findTagInBucket(int i, int tag, TagPosition position) {
    for (int j = 0; j < mTagsPerBucket; j++) {
      if (readTag(i, j) == tag) {
        position.setBucketIndex(i);
        position.setTagIndex(j);
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean findTagInBuckets(int i1, int i2, int tag, TagPosition position) {
    for (int j = 0; j < mTagsPerBucket; j++) {
      if (readTag(i1, j) == tag) {
        position.setBucketIndex(i1);
        position.setTagIndex(j);
        return true;
      } else if (readTag(i2, j) == tag) {
        position.setBucketIndex(i2);
        position.setTagIndex(j);
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean deleteTagFromBucket(int i, int tag, TagPosition position) {
    for (int j = 0; j < mTagsPerBucket; j++) {
      if (readTag(i, j) == tag) {
        writeTag(i, j, 0);
        position.setBucketIndex(i);
        position.setTagIndex(j);
        return true;
      }
    }
    return false;
  }

  @Override
  public int insertOrKickoutOne(int i, int tag, TagPosition position) {
    for (int j = 0; j < mTagsPerBucket; j++) {
      if (readTag(i, j) == 0) {
        writeTag(i, j, tag);
        position.setBucketIndex(i);
        position.setTagIndex(j);
        return 0;
      }
    }
    int r = ThreadLocalRandom.current().nextInt(mTagsPerBucket);
    int oldTag = readTag(i, r);
    writeTag(i, r, tag);
    position.setBucketIndex(i);
    position.setTagIndex(r);
    return oldTag;
  }

  @Override
  public boolean insert(int i, int tag, TagPosition position) {
    for (int j = 0; j < mTagsPerBucket; j++) {
      if (readTag(i, j) == 0) {
        writeTag(i, j, tag);
        position.setBucketIndex(i);
        position.setTagIndex(j);
        return true;
      }
    }
    return false;
  }

  @Override
  public int numTagsPerBuckets() {
    return mTagsPerBucket;
  }

  @Override
  public int numBuckets() {
    return this.mNumBuckets;
  }

  @Override
  public int bitsPerTag() {
    return mBitsPerTag;
  }

  @Override
  public int sizeInBytes() {
    return this.mBits.size() >> 3;
  }

  @Override
  public int sizeInTags() {
    return this.mNumBuckets * mTagsPerBucket;
  }

  private int getTagOffset(int bucketIndex, int posInBucket) {
    return (bucketIndex * mTagsPerBucket * mBitsPerTag) + (posInBucket * mBitsPerTag);
  }
}
