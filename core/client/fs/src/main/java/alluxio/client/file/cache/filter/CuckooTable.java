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

/**
 * The cuckoo table that supports to store and access data from <bucket, slot>.
 */
public interface CuckooTable {
  /**
   * Read the value of tag in specified position.
   *
   * @param i the bucket index
   * @param j the slot in bucket
   * @return the tag value to read
   */
  public int readTag(int i, int j);

  /**
   * Set the value of tag in specified position.
   *
   * @param i the bucket index
   * @param j the slot in bucket
   * @param t the tag value to write
   */
  public void writeTag(int i, int j, int t);

  /**
   * Find a tag in specified bucket.
   *
   * @param i the bucket index
   * @param tag the tag value to find
   * @return true if the tag is found; false otherwise
   */
  public boolean findTagInBucket(int i, int tag);

  /**
   * Find a tag in specified bucket and record its position.
   *
   * @param i the bucket index
   * @param tag the tag value to find
   * @param position the detailed position of found tag
   * @return true if the tag is found; false otherwise
   */
  public boolean findTagInBucket(int i, int tag, TagPosition position);

  /**
   * Find a tag in specified two buckets.
   *
   * @param i1 the first bucket index
   * @param i2 the second bucket index
   * @param tag the tag value to find
   * @return true if the tag is found; false otherwise
   */
  public boolean findTagInBuckets(int i1, int i2, int tag);

  /**
   * Find a tag in specified two buckets and record its position.
   *
   * @param i1 the first bucket index
   * @param i2 the second bucket index
   * @param tag the tag value to find
   * @param position the detailed position of found tag
   * @return true if the tag is found; false otherwise
   */
  public boolean findTagInBuckets(int i1, int i2, int tag, TagPosition position);

  /**
   * Delete a tag in specified bucket.
   *
   * @param i the bucket index
   * @param tag the tag value to find
   * @return true if the tag is deleted; false otherwise
   */
  public boolean deleteTagFromBucket(int i, int tag);

  /**
   * Delete a tag in specified bucket and record its position.
   *
   * @param i the bucket to delete from
   * @param tag the tag value to find
   * @param position the detailed position of deleted tag
   * @return true if the tag is found; false otherwise
   */
  public boolean deleteTagFromBucket(int i, int tag, TagPosition position);

  /**
   * Insert a tag into specified bucket. If no empty slot found, it will kickout one randomly and
   * return the victim's tag value.
   *
   * @param i the bucket index
   * @param tag the tag value to find
   * @return the tag value kicked out
   */
  public int insertOrKickoutOne(int i, int tag);

  /**
   * Insert a tag into specified bucket. If no empty slot found, it will kickout one randomly,
   * return the victim's tag value, and record its position.
   *
   * @param i the bucket want to insert
   * @param tag the tag value to find
   * @param position the detailed position of deleted tag
   * @return the tag value kicked out
   */
  public int insertOrKickoutOne(int i, int tag, TagPosition position);

  /**
   * Insert a tag into specified bucket and record its position.
   *
   * @param i the bucket want to insert
   * @param tag the tag value to find
   * @param position the detailed position of deleted tag
   * @return true if the tag is successfully inserted; false otherwise
   */
  public boolean insert(int i, int tag, TagPosition position);

  /**
   * @return the number of tags per bucket
   */
  public int getNumTagsPerBuckets();

  /**
   * @return the number of buckets
   */
  public int getNumBuckets();

  /**
   * @return the number of bits per tag
   */
  public int getBitsPerTag();

  /**
   * @return the number of bytes
   */
  public int getSizeInBytes();

  /**
   * @return the number of tags
   */
  public int getSizeInTags();
}
