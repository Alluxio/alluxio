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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.concurrent.LockMode;
import alluxio.resource.LockResource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class LockPoolTest {
  private LockPool<Integer> mCache;

  private static final int INITIAL_CAP = 10;
  private static final int CONCURRENCY_LEVEL = 10;

  @Before
  public void before() {
    mCache = new LockPool<>(INITIAL_CAP, CONCURRENCY_LEVEL);
  }

  @Test
  public void referenceTest() {
    assertTrue(mCache.get(1, LockMode.WRITE).hasSameLock(mCache.get(1, LockMode.WRITE)));
    assertFalse(mCache.get(1, LockMode.WRITE).hasSameLock(mCache.get(2, LockMode.WRITE)));
  }

  @Test
  public void testReadLock() throws Exception {
    try (LockResource lock = mCache.get(1, LockMode.READ)) {
      System.gc();
      CompletableFuture.runAsync(() -> {
        try (LockResource innerLock = mCache.get(1, LockMode.READ)) {
          assertTrue(lock.hasSameLock(innerLock));
        }
      }).get();
    }
  }

  @Test
  public void testTryLockRead() throws Exception {
    try (LockResource lock = mCache.tryGet(1, LockMode.READ).get()) {
      System.gc();
      CompletableFuture.runAsync(() -> {
        try (LockResource innerLock = mCache.tryGet(1, LockMode.READ).get()) {
          assertTrue(lock.hasSameLock(innerLock));
        }
      }).get();
    }
  }

  @Test
  public void testTryLockWrite() throws Exception {
    try (LockResource ignored = mCache.tryGet(1, LockMode.WRITE).get()) {
      System.gc();
      CompletableFuture.runAsync(() ->
          assertFalse(mCache.tryGet(1, LockMode.WRITE).isPresent())).get();
    }
  }

  @Test
  public void testWriteReadLock() {
    assertThrows(TimeoutException.class, () -> {
      try (LockResource ignored = mCache.get(1, LockMode.WRITE)) {
        System.gc();
        CompletableFuture.runAsync(() -> {
          try (LockResource ignored1 = mCache.get(1, LockMode.READ)) {
            // should be blocked
            throw new RuntimeException();
          }
        }).get(1000, TimeUnit.MILLISECONDS);
      }
    });
  }

  @Test
  public void testReadWriteLock() {
    assertThrows(TimeoutException.class, () -> {
      try (LockResource ignored = mCache.get(1, LockMode.READ)) {
        System.gc();
        CompletableFuture.runAsync(() -> {
          try (LockResource ignored1 = mCache.get(1, LockMode.WRITE)) {
            // should be blocked
            throw new RuntimeException();
          }
        }).get(1000, TimeUnit.MILLISECONDS);
      }
    });
  }

  @Test
  public void testWriteWriteLock() {
    assertThrows(TimeoutException.class, () -> {
      try (LockResource ignored = mCache.get(1, LockMode.WRITE)) {
        System.gc();
        CompletableFuture.runAsync(() -> {
          try (LockResource ignored1 = mCache.get(1, LockMode.WRITE)) {
            // should be blocked
            throw new RuntimeException();
          }
        }).get(1000, TimeUnit.MILLISECONDS);
      }
    });
  }

  private String getResourceId(int lockId) {
    try (LockResource writeResource = mCache.get(lockId, LockMode.WRITE)) {
      return writeResource.getLockIdentity();
    }
  }

  @Test
  public void gcWithReferenceTest() {
    LockResource resource = mCache.get(2, LockMode.WRITE);
    String firstId = getResourceId(2);
    System.gc();
    assertEquals(firstId, getResourceId(2));
    assertTrue(resource.hasSameLock(mCache.get(2, LockMode.WRITE)));
  }

  @Test
  public void clearValues() {
    final int addCount = 1000000;
    for (int i = 0; i < addCount; i++) {
      getResourceId(i);
    }
    System.gc();
    for (int i = addCount; i < addCount + addCount; i++) {
      getResourceId(i);
    }
    System.gc();
    assertTrue(mCache.size() < addCount);
  }

  @Test
  public void gcWithoutReferenceTest() {
    String firstId = getResourceId(1);
    System.gc();
    String secondId = getResourceId(1);
    assertNotEquals(firstId, secondId);
  }

  @After
  public void after() throws Exception {
    mCache.close();
  }
}
