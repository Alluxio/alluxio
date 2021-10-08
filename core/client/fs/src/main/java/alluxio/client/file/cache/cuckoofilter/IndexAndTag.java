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
 * This class stores the bucket index and tag of an item.
 */
final class IndexAndTag {
  public final int mBucketIndex;
  public final int mTag;

  /**
   * @param bucketIndex the bucket index
   * @param tag the tag value
   */
  IndexAndTag(int bucketIndex, int tag) {
    mBucketIndex = bucketIndex;
    mTag = tag;
  }
}
