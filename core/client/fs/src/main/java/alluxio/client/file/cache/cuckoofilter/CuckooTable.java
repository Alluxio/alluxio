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
 * The cuckoo table that supports to store and access data positioned by a specific bucket and slot.
 */
public interface CuckooTable {
  /**
   * Reads the value of tag in specified position.
   *
   * @param bucketIndex the bucket index
   * @param slotIndex the slot in bucket
   * @return the tag value to read
   */
  int readTag(int bucketIndex, int slotIndex);

  /**
   * Set the value of tag in specified position.
   *
   * @param bucketIndex the bucket index
   * @param slotIndex the slot in bucket
   * @param tag the tag value to write
   */
  void writeTag(int bucketIndex, int slotIndex, int tag);

  /**
   * Find a tag in specified bucket and return its position.
   *
   * @param bucketIndex the bucket index
   * @param tag the tag value to find
   * @return the valid position of this tag if it is found; otherwise an invalid position indicates
   *         that tag is not found
   */
  TagPosition findTag(int bucketIndex, int tag);

  /**
   * Find a tag in specified two buckets and return its position.
   *
   * @param bucketIndex1 the first bucket index
   * @param bucketIndex2 the second bucket index
   * @param tag the tag value to find
   * @return the valid position of this tag if it is found; otherwise an invalid position indicates
   *         that tag is not found
   */
  TagPosition findTag(int bucketIndex1, int bucketIndex2, int tag);

  /**
   * Delete a tag in specified bucket and return its position.
   *
   * @param bucketIndex the bucket to delete from
   * @param tag the tag value to find
   * @return the valid position of this tag if it is found and deleted; otherwise an invalid
   *         position indicates that tag is not found
   */
  TagPosition deleteTag(int bucketIndex, int tag);

  /**
   * Insert a tag into specified bucket. If no empty slot found, it will kickout one randomly and
   * return the victim's tag value.
   *
   * @param bucketIndex the bucket index
   * @param tag the tag value to find
   * @return the tag value kicked out
   */
  int insertOrKickTag(int bucketIndex, int tag);

  /**
   * @return the number of tags per bucket
   */
  int getNumTagsPerBuckets();

  /**
   * @return the number of buckets
   */
  int getNumBuckets();

  /**
   * @return the number of bits per tag
   */
  int getBitsPerTag();

  /**
   * @return the number of bytes
   */
  int getSizeInBytes();

  /**
   * @return the number of tags
   */
  int getSizeInTags();
}
