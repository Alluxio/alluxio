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

/**
 * This class represents a position of a tag.
 */
public class TagPosition {
  public int mBucketIndex;
  public int mSlotIndex;
  public CuckooStatus mStatus;

  /**
   * Create a tag position with default parameter.
   */
  public TagPosition() {
    this(-1, -1, CuckooStatus.UNDEFINED);
  }

  /**
   * Create a tag position with given position.
   *
   * @param bucketIndex the bucket index
   * @param slotIndex the slot
   */
  public TagPosition(int bucketIndex, int slotIndex) {
    this(bucketIndex, slotIndex, CuckooStatus.UNDEFINED);
  }

  /**
   * Create a tag position with given position and status.
   *
   * @param bucketIndex the bucket index
   * @param slotIndex the slot
   * @param status the status
   */
  public TagPosition(int bucketIndex, int slotIndex, CuckooStatus status) {
    mBucketIndex = bucketIndex;
    mSlotIndex = slotIndex;
    mStatus = status;
  }

  /**
   * @return true is this tag position represents a valid position
   */
  boolean valid() {
    return mBucketIndex >= 0 && mSlotIndex >= 0;
  }

  /**
   * @return the bucket index
   */
  public int getBucketIndex() {
    return mBucketIndex;
  }

  /**
   * Set the bucket index.
   *
   * @param bucketIndex the bucket index
   */
  public void setBucketIndex(int bucketIndex) {
    mBucketIndex = bucketIndex;
  }

  /**
   * @return the slot index
   */
  public int getSlotIndex() {
    return mSlotIndex;
  }

  /**
   * Set the slot index.
   *
   * @param slotIndex the slot
   */
  public void setSlotIndex(int slotIndex) {
    mSlotIndex = slotIndex;
  }

  /**
   * @return the status of this tag position
   */
  public CuckooStatus getStatus() {
    return mStatus;
  }

  /**
   * Set the status of this tag position.
   *
   * @param status the status
   */
  public void setStatus(CuckooStatus status) {
    mStatus = status;
  }

  /**
   * Set the bucket and slot.
   *
   * @param bucket the bucket
   * @param slot the slot
   */
  public void setBucketAndSlot(int bucket, int slot) {
    mBucketIndex = bucket;
    mSlotIndex = slot;
  }

  @Override
  public String toString() {
    return "TagPosition{" + "bucketIndex=" + mBucketIndex + ", tagIndex=" + mSlotIndex + '}';
  }
}
