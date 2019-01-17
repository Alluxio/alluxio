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

package alluxio.collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests the {@link LockCache} class.
 */
public class LockCacheTest {
  private LockCache<Integer, Integer> mCache;
  private static final int MAX_SIZE = 16;

  /**
   * Sets up the fields before running a test.
   */
  @Before
  public void before() {
    mCache = new LockCache<>(k -> new Integer(k), 2, MAX_SIZE, 4);
  }

  @Test(timeout = 10000)
  public void insertValueTest() {
    int highWaterMark = Math.round(LockCache.SOFT_LIMIT_RATIO * MAX_SIZE);

    for (int i = 0; i < highWaterMark; i++) {
      assertEquals(i , mCache.size());
      LockCache<Integer, Integer>.ValNode<Integer> valNode = mCache.get(i);
      assertEquals(i, valNode.get().intValue());
      assertTrue(mCache.size() < MAX_SIZE);
      valNode.getRefCounter().decrementAndGet();
    }
    for (int i = highWaterMark; i < MAX_SIZE; i++) {
      mCache.get(i).getRefCounter().decrementAndGet();
    }
    // it should be full now
    for (int i = highWaterMark; i < 2 * MAX_SIZE; i++) {
      LockCache<Integer, Integer>.ValNode<Integer> valNode = mCache.get(i);
      assertEquals(i, valNode.get().intValue());
      assertTrue(mCache.contains(i));
      assertTrue(mCache.size() <= MAX_SIZE);
      valNode.getRefCounter().decrementAndGet();
    }
  }

  private void insert(int low, int high, int totalThreadCount) {
    Thread t = new Thread(() -> {
      for (int i = low; i < high; i++) {
        int val = mCache.get(i).get();
        assertTrue(mCache.size() < MAX_SIZE);
        assertTrue(mCache.contains(i));
        assertEquals(i, val);
      }
    });
    t.start();
  }

  @Test
  public void parallelInsertTest() {
    insert(0, MAX_SIZE * 2, 4);
    insert(0, MAX_SIZE * 2, 4);
    insert(MAX_SIZE * 2, MAX_SIZE * 4, 4);
    insert(MAX_SIZE * 2, MAX_SIZE * 4, 4);
  }
}
