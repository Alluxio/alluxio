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

package alluxio.resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Unit tests for {@link LockResource}.
 */
public final class LockResourceTest {

  /**
   * Tests {@link LockResource} with {@link ReentrantLock}.
   */
  @Test
  public void reentrantLock() {
    Lock lock = new ReentrantLock();
    try (LockResource r1 = new LockResource(lock)) {
      try (LockResource r2 = new LockResource(lock)) {
        assertTrue(lock.tryLock());
        lock.unlock();
      }
    }
  }

  /**
   * Tests {@link LockResource} with {@link ReentrantLock}.
   */
  @Test
  public void reentrantTryLock() {
    Lock lock = new ReentrantLock();
    try (LockResource r1 = new LockResource(lock, true, true)) {
      try (LockResource r2 = new LockResource(lock, true, true)) {
        assertTrue(lock.tryLock());
        lock.unlock();
      }
    }
  }

  /**
   * Tests {@link LockResource} with {@link ReentrantReadWriteLock}.
   */
  @Test
  public void reentrantReadWriteLock() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    try (LockResource r1 = new LockResource(lock.readLock())) {
      try (LockResource r2 = new LockResource(lock.readLock())) {
        assertEquals(lock.getReadHoldCount(), 2);
        assertTrue(lock.readLock().tryLock());
        lock.readLock().unlock();
      }
    }
    assertEquals(lock.getReadHoldCount(), 0);

    try (LockResource r1 = new LockResource(lock.writeLock())) {
      try (LockResource r2 = new LockResource(lock.readLock())) {
        assertTrue(lock.isWriteLockedByCurrentThread());
        assertEquals(lock.getReadHoldCount(), 1);
      }
    }
    assertFalse(lock.isWriteLockedByCurrentThread());
    assertEquals(lock.getReadHoldCount(), 0);

    try (LockResource r = new LockResource(lock.readLock())) {
      assertFalse(lock.writeLock().tryLock());
    }
  }
}
