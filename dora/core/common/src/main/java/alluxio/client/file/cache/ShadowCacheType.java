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

package alluxio.client.file.cache;

/**
 * This represents the different shadow cache implementations that can be instantiated.
 */
public enum ShadowCacheType {
  /**
   * A shadow cache with multiple bloom filter implementation. It creates a chain of bloom filters
   * that support updating operation which updates one of them during each sub-window, and switch
   * operation which switch to the least recently used BF and clear it on switching sub-window.
   */
  MULTIPLE_BLOOM_FILTER,
  /**
   * A shadow cache with clock cuckoo filter implementation. It creates a cuckoo filter with
   * extended field (clock, size, etc.). Thanks to the deletable feature of cuckoo filter, it can
   * adapt to dynamic workload more smoothly. Clock cuckoo filter will `aging` all entries
   * periodically. Specifically, it scans all stored entries, decreases their clock value, and
   * deletes the stale ones whose clock value are reduced to zero. This process is similar to the
   * well-known CLOCK algorithm, which is why we name it clock cuckoo filter.
   */
  CLOCK_CUCKOO_FILTER,
}
