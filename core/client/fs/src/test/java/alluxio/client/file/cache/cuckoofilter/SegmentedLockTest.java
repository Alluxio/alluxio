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

import alluxio.test.util.ConcurrencyUtils;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class SegmentedLockTest {
  private static final int NUM_BUCKETS = 1024;
  private static final int NUM_LOCKS = 128;

  private static final int DEFAULT_THREAD_AMOUNT = 12;
  private static final int DEFAULT_TIMEOUT_SECONDS = 10;

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
  public void testConcurrency() throws Exception {
    List<Runnable> runnables = new ArrayList<>();
    for (int k = 0; k < DEFAULT_THREAD_AMOUNT; k++) {
      runnables.add(() -> {
        for (int i = 0; i < NUM_BUCKETS; i++) {
          int r1 = ThreadLocalRandom.current().nextInt(NUM_BUCKETS);
          int r2 = ThreadLocalRandom.current().nextInt(NUM_BUCKETS);
          mLocks.writeLock(r1, r2);
          mLocks.unlockWrite(r1, r2);
        }
      });
    }
    ConcurrencyUtils.assertConcurrent(runnables, DEFAULT_TIMEOUT_SECONDS);
  }
}
