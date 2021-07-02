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
import alluxio.util.CommonUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Tests the {@link LockPool} class.
 */
public class LockPoolTest {
  private LockPool<Integer> mPool;
  private static final int LOW_WATERMARK = 8;
  private static final int HIGH_WATERMARK = 16;

  /**
   * Sets up the fields before running a test.
   */
  @Before
  public void before() {
    mPool = new LockPool<>(k -> new ReentrantReadWriteLock(), 2, LOW_WATERMARK, HIGH_WATERMARK, 4);
  }

  @After
  public void after() throws Exception {
    mPool.close();
  }

  @Test(timeout = 10000)
  public void insertValueTest() throws Exception {
    // Fills cache until high watermark.
    for (int key = 0; key < HIGH_WATERMARK; key++) {
      assertEquals(key , mPool.size());
      try (LockResource resource = mPool.get(key, LockMode.READ)) {
        assertTrue(mPool.containsKey(key));
        assertEquals(key + 1, mPool.size());
      }
    }
    // Exceeds high watermark, will be evicted until low watermark.
    try (LockResource r = mPool.get(HIGH_WATERMARK, LockMode.READ)) {
      assertTrue(mPool.containsKey(HIGH_WATERMARK));
      CommonUtils.waitFor("Pool size to go below low watermark",
          () -> mPool.size() <= LOW_WATERMARK);
      assertEquals(LOW_WATERMARK, mPool.size());
    }
    // Fills cache until high watermark again.
    for (int newLock = 1; newLock <= HIGH_WATERMARK - LOW_WATERMARK; newLock++) {
      int key = HIGH_WATERMARK + newLock;
      try (LockResource resource = mPool.get(key, LockMode.READ)) {
        assertTrue(mPool.containsKey(key));
        assertEquals(LOW_WATERMARK + newLock, mPool.size());
      }
    }
    // Exceeds high watermark, will be evicted until low watermark.
    try (LockResource r = mPool.get(2 * HIGH_WATERMARK, LockMode.READ)) {
      assertTrue(mPool.containsKey(2 * HIGH_WATERMARK));
      CommonUtils.waitFor("Pool size to go below low watermark",
          () -> mPool.size() <= LOW_WATERMARK);
      assertEquals(LOW_WATERMARK, mPool.size());
    }
  }

  private Thread getKeys(int low, int high) {
    Thread t = new Thread(() -> {
      for (int i = low; i < high; i++) {
        try (LockResource resource = mPool.get(i, LockMode.READ)) {
          // Empty.
        }
      }
    });
    t.start();
    return t;
  }

  @Test(timeout = 1000)
  public void parallelInsertTest() throws Exception {
    // Fills in the pool.
    Thread t1 = getKeys(0, HIGH_WATERMARK);
    Thread t2 = getKeys(0, HIGH_WATERMARK);
    t1.join();
    t2.join();

    assertEquals(HIGH_WATERMARK, mPool.size());
    for (int key = 0; key < HIGH_WATERMARK; key++) {
      assertTrue(mPool.containsKey(key));
    }

    // Evicts the old locks until size goes below low watermark.
    Thread t3 = getKeys(HIGH_WATERMARK, HIGH_WATERMARK + 1);
    t3.join();

    CommonUtils.waitFor("Pool size to go below low watermark",
        () -> mPool.size() <= LOW_WATERMARK);
    assertEquals(LOW_WATERMARK, mPool.size());

    // Fills in the pool again.
    int newStartKey = HIGH_WATERMARK + 1;
    int newEndKey = newStartKey + HIGH_WATERMARK - LOW_WATERMARK;
    Thread t4 = getKeys(newStartKey, newEndKey);
    Thread t5 = getKeys(newStartKey, newEndKey);
    t4.join();
    t5.join();

    assertEquals(HIGH_WATERMARK, mPool.size());
    for (int key = newStartKey; key < newEndKey; key++) {
      assertTrue(Integer.toString(key), mPool.containsKey(key));
    }
  }

  @Test(timeout = 1000)
  public void referencedLockTest() {
    LockResource lock0 = mPool.get(0, LockMode.READ);
    LockResource lock1 = mPool.get(50, LockMode.READ);
    LockResource lock2 = mPool.get(100, LockMode.READ);

    for (int j = 0; j < 10; j++) {
      for (int i = 0; i < 100; i++) {
        mPool.get(i, LockMode.READ).close();
      }
    }
    assertTrue(lock0.hasSameLock(mPool.get(0, LockMode.READ)));
    assertTrue(lock1.hasSameLock(mPool.get(50, LockMode.READ)));
    assertTrue(lock2.hasSameLock(mPool.get(100, LockMode.READ)));
  }
}
