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

enum CuckooStatus {
  OK(0), FAILURE(1), FAILURE_KEY_NOT_FOUND(2), FAILURE_KEY_DUPLICATED(3), FAILURE_TABLE_FULL(
      4), UNDEFINED(5);

  public int code;

  CuckooStatus(int code) {
    this.code = code;
  }
}


public class TagPosition {
  public int bucketIndex;
  public int tagIndex;
  public CuckooStatus status;

  public TagPosition() {
    this(-1, -1, CuckooStatus.UNDEFINED);
  }

  public TagPosition(int bucketIndex, int tagIndex) {
    this(bucketIndex, tagIndex, CuckooStatus.UNDEFINED);
  }

  public TagPosition(int bucketIndex, int tagIndex, CuckooStatus status) {
    this.bucketIndex = bucketIndex;
    this.tagIndex = tagIndex;
    this.status = status;
  }

  boolean valid() {
    return bucketIndex >= 0 && tagIndex >= 0;
  }

  public int getBucketIndex() {
    return bucketIndex;
  }

  public void setBucketIndex(int bucketIndex) {
    this.bucketIndex = bucketIndex;
  }

  public int getTagIndex() {
    return tagIndex;
  }

  public void setTagIndex(int tagIndex) {
    this.tagIndex = tagIndex;
  }

  public CuckooStatus getStatus() {
    return status;
  }

  public void setStatus(CuckooStatus status) {
    this.status = status;
  }

  public void setBucketAndSlot(int bucket, int slot) {
    this.bucketIndex = bucket;
    this.tagIndex = slot;
  }

  @Override
  public String toString() {
    return "TagPosition{" + "bucketIndex=" + bucketIndex + ", tagIndex=" + tagIndex + '}';
  }
}
