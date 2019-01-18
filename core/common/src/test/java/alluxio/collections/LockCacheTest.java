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

import alluxio.resource.LockResource;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Tests the {@link LockCache} class.
 */
public class LockCacheTest {
  private LockCache<Integer> mCache;
  private static final int MAX_SIZE = 16;

  /**
   * Sets up the fields before running a test.
   */
  @Before
  public void before() {
    mCache = new LockCache<>(k -> new ReentrantReadWriteLock(), 2, MAX_SIZE, 4);
  }

  @Test(timeout = 10000)
  public void insertValueTest() {
    int highWaterMark = Math.round(LockCache.SOFT_LIMIT_RATIO * MAX_SIZE);

    for (int i = 0; i < highWaterMark; i++) {
      assertEquals(i , mCache.size());
      try (LockResource resource = mCache.get(i, LockResource.LockMode.READ)) {
        assertTrue(mCache.contains(i));
        assertTrue(mCache.size() < MAX_SIZE);
      }
    }

    // it should be full now
    for (int i = highWaterMark; i < 2 * MAX_SIZE; i++) {
      try (LockResource resource = mCache.get(i, LockResource.LockMode.READ)) {
        assertTrue(mCache.contains(i));
        assertTrue(mCache.size() <= MAX_SIZE);
      }
    }
  }

  private Thread insert(int low, int high, int totalThreadCount) {
    Thread t = new Thread(() -> {
      for (int i = low; i < high; i++) {
        try (LockResource resource = mCache.get(i, LockResource.LockMode.READ)) {
          assertTrue(mCache.size() <= MAX_SIZE + totalThreadCount);
          assertTrue(mCache.contains(i));
        }
      }
    });
    t.start();
    return t;
  }

  @Test
  public void parallelInsertTest() throws InterruptedException {
    Thread t1 = insert(0, MAX_SIZE * 2, 4);
    Thread t2 = insert(0, MAX_SIZE * 2, 4);
    Thread t3 = insert(MAX_SIZE * 2, MAX_SIZE * 4, 4);
    Thread t4 = insert(MAX_SIZE * 2, MAX_SIZE * 4, 4);
    t1.join();
    t2.join();
    t3.join();
    t4.join();
  }
}
