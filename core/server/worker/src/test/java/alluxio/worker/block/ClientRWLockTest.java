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

package alluxio.worker.block;

import static junit.framework.TestCase.assertNotSame;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.locks.Lock;

/**
 * Unit tests for {@link ClientRWLock}.
 */
public final class ClientRWLockTest {

  private ClientRWLock mClientRWLock;
  private Lock mReadLock;
  private Lock mWriteLock;

  /**
   * Sets up the constructors before a test run.
   */
  @Before
  public void before() {
    mClientRWLock = new ClientRWLock();
    mReadLock = mClientRWLock.readLock();
    mWriteLock = mClientRWLock.writeLock();
  }

  /**
   * Tests the equality for both {@link ClientRWLock} constructors.
   */
  @Test
  public void notSameLock() {
    assertNotSame(mReadLock, mWriteLock);
  }

  /**
   * Tests the {@link ClientRWLock#unlock()} method.
   */
  @Test
  public void unlock() throws Exception {
    mReadLock.unlock();
    assertTrue(true);
  }

  /**
   * Tests the {@link ClientRWLock#tryLock()} method.
   */
  @Test
  public void tryLockTestFail() throws Exception {
    mWriteLock.lock();
    assertFalse(mWriteLock.tryLock());
  }

  /**
   * Tests the {@link ClientRWLock#lockInterruptibly()} method.
   */
  @Test
  public void lockInterruptibly() throws Exception {
    mReadLock.lockInterruptibly();
    assertTrue(true);
  }

  /**
   * Tests reference counting.
   */
  @Test
  public void referenceCounting() throws Exception {
    assertEquals(0, mClientRWLock.getReferenceCount());
    mClientRWLock.addReference();
    mClientRWLock.addReference();
    assertEquals(2, mClientRWLock.getReferenceCount());
    assertEquals(1, mClientRWLock.dropReference());
    for (int i = 0; i < 10; i++) {
      mClientRWLock.addReference();
    }
    assertEquals(11, mClientRWLock.getReferenceCount());
    assertEquals(10, mClientRWLock.dropReference());
    assertEquals(10, mClientRWLock.getReferenceCount());
  }
}
