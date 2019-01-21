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

import alluxio.concurrent.LockMode;
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

  @Test(timeout = 1000)
  public void insertValueTest() {
    int highWaterMark = mCache.getSoftLimit();

    for (int i = 0; i < highWaterMark; i++) {
      assertEquals(i , mCache.size());
      try (LockResource resource = mCache.get(i, LockMode.READ)) {
        assertTrue(mCache.containsKey(i));
        assertTrue(mCache.size() < MAX_SIZE);
      }
    }

    // it should be full now
    for (int i = highWaterMark; i < 2 * MAX_SIZE; i++) {
      try (LockResource resource = mCache.get(i, LockMode.READ)) {
        assertTrue(mCache.containsKey(i));
        assertTrue(mCache.size() <= MAX_SIZE);
      }
    }
  }

  private Thread getKeys(int low, int high, int totalThreadCount) {
    Thread t = new Thread(() -> {
      for (int i = low; i < high; i++) {
        try (LockResource resource = mCache.get(i, LockMode.READ)) {
          assertTrue(mCache.size() <= MAX_SIZE + totalThreadCount);
          assertTrue(mCache.containsKey(i));
        }
      }
    });
    t.start();
    return t;
  }

  @Test(timeout = 1000)
  public void parallelInsertTest() throws InterruptedException {
    Thread t1 = getKeys(0, MAX_SIZE * 2, 4);
    Thread t2 = getKeys(0, MAX_SIZE * 2, 4);
    Thread t3 = getKeys(MAX_SIZE * 2, MAX_SIZE * 4, 4);
    Thread t4 = getKeys(MAX_SIZE * 2, MAX_SIZE * 4, 4);
    t1.join();
    t2.join();
    t3.join();
    t4.join();
  }

  @Test(timeout = 1000)
  public void referencedLockTest() throws InterruptedException {
    LockResource lock0 = mCache.get(0, LockMode.READ);
    LockResource lock1 = mCache.get(50, LockMode.READ);
    LockResource lock2 = mCache.get(100, LockMode.READ);

    for (int j = 0; j < 10; j++) {
      for (int i = 0; i < 100; i++) {
        mCache.get(i, LockMode.READ).close();
      }
    }
    assertTrue(lock0.hasSameLock(mCache.get(0, LockMode.READ)));
    assertTrue(lock1.hasSameLock(mCache.get(50, LockMode.READ)));
    assertTrue(lock2.hasSameLock(mCache.get(100, LockMode.READ)));
  }
}
