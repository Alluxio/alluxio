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

import com.google.common.math.DoubleMath;

import java.math.RoundingMode;

/**
 * This class provides some useful methods for cuckoo filters.
 */
public class CuckooUtils {

  /**
   * Computes the bucket index on given hash value.
   *
   * @param hv the hash value
   * @param numBuckets the number of buckets
   * @return the bucket computed
   */
  public static int indexHash(int hv, int numBuckets) {
    return hv & (numBuckets - 1);
  }

  /**
   * Compute the alternative index on given hash value and tag.
   *
   * @param index the bucket index
   * @param tag the fingerprint
   * @param numBuckets the number of buckets
   * @return the alternative bucket index computed
   */
  public static int altIndex(int index, int tag, int numBuckets) {
    return CuckooUtils.indexHash((int) (index ^ (tag * 0x5bd1e995)), numBuckets);
  }

  /**
   * Compute the fingerprint on given hash value.
   *
   * @param hv the hash value
   * @param bitsPerTag the number of bits each tag has
   * @return the fingerprint
   */
  public static int tagHash(int hv, int bitsPerTag) {
    int tag;
    tag = hv & ((1 << bitsPerTag) - 1);
    return tag == 0 ? 1 : tag;
  }

  /**
   * Compute the number of bits of each tag on given fpp. The equation is from "Cuckoo Filter:
   * Practically Better Than Bloom" by Fan et al.
   *
   * @param fpp the false positive probability
   * @param loadFactor the load factor
   * @return the optimized number of bits
   */
  public static int optimalBitsPerTag(double fpp, double loadFactor) {
    return DoubleMath.roundToInt(DoubleMath.log2((1 / fpp) + 3) / loadFactor, RoundingMode.UP);
  }

  /**
   * Compute the number of buckets using the expected number of insertions, the load factor, and the
   * number of tags per bucket.
   *
   * @param expectedInsertions the expected number of unique items
   * @param loadFactor the load factor
   * @param tagsPerBucket the number of slots per bucket has
   * @return the optimized number of buckets
   */
  public static long optimalBuckets(long expectedInsertions, double loadFactor, int tagsPerBucket) {
    long bucketsNeeded = DoubleMath
        .roundToLong((1.0 / loadFactor) * expectedInsertions / tagsPerBucket, RoundingMode.UP);
    // get next biggest power of 2
    long bitPos = Long.highestOneBit(bucketsNeeded);
    if (bucketsNeeded > bitPos) {
      bitPos = bitPos << 1;
    }
    return bitPos;
  }
}
