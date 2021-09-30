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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

public class SegmentedLockTest {
  private static final int NUM_BUCKETS = 1024;
  private static final int NUM_LOCKS = 128;

  private SegmentedLock mLocks;

  @Before
  public void init() {
    create(NUM_LOCKS, NUM_BUCKETS);
  }

  private void create(int numLocks, int numBuckets) {
    mLocks = new SegmentedLock(NUM_LOCKS, NUM_BUCKETS);
  }

  @Test
  public void testConstruct() {
    create(1023, 128);
    assertEquals(128, mLocks.getNumLocks());

    create(1023, 127);
    assertEquals(128, mLocks.getNumLocks());

    create(513, 65);
    assertEquals(128, mLocks.getNumLocks());
  }

  @Test
  public void testConcurrency() {
    for (int i = 0; i < NUM_BUCKETS; i++) {
      int r1 = ThreadLocalRandom.current().nextInt(NUM_BUCKETS);
      int r2 = ThreadLocalRandom.current().nextInt(NUM_BUCKETS);
      mLocks.lockTwoWrite(r1, r2);
      mLocks.unlockTwoWrite(r1, r2);
    }
  }
}
