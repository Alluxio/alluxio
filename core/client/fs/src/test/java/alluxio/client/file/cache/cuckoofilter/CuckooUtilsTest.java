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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import org.junit.Test;

public class CuckooUtilsTest {
  private static final int NUM_BUCKETS = 16;
  private static final int TAGS_PER_BUCKET = 4;
  private static final int BITS_PER_TAG = 8;

  @Test
  public void testIndexAndTagHash() {
    for (int i = 0; i < NUM_BUCKETS; i++) {
      for (int j = 0; j < TAGS_PER_BUCKET; j++) {
        long hv = Hashing.murmur3_128().newHasher()
            .putObject(i * NUM_BUCKETS + j, Funnels.integerFunnel()).hash().asLong();
        int index = CuckooUtils.indexHash((int) (hv >> 32), NUM_BUCKETS);
        int tag = CuckooUtils.tagHash((int) hv, BITS_PER_TAG);
        assertTrue(0 <= index && index < NUM_BUCKETS);
        assertTrue(0 < tag && tag <= ((1 << BITS_PER_TAG) - 1));
        int altIndex = CuckooUtils.altIndex(index, tag, NUM_BUCKETS);
        int altAltIndex = CuckooUtils.altIndex(altIndex, tag, NUM_BUCKETS);
        assertEquals(index, altAltIndex);
      }
    }
  }
}
