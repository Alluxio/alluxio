/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.worker.block;

import java.util.concurrent.locks.Lock;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link ClientRWLock}.
 */
public final class ClientRWLockTest {

  private Lock mReadLock;
  private Lock mWriteLock;

  /**
   * Sets up the constructors before a test run.
   */
  @Before
  public void before() {
    mReadLock = new ClientRWLock().readLock();
    mWriteLock = new ClientRWLock().writeLock();
  }

  /**
   * Tests the equality for both {@link ClientRWLock} constructors.
   */
  @Test
  public void notSameLockTest() {
    Assert.assertNotSame(mReadLock, mWriteLock);
  }

  /**
   * Tests the {@link ClientRWLock#unlock()} method.
   */
  @Test
  public void unlockTest() throws Exception {
    mReadLock.unlock();
    Assert.assertTrue(true);
  }

  /**
   * Tests the {@link ClientRWLock#tryLock()} method.
   */
  @Test
  public void tryLockTestFail() throws Exception {
    mWriteLock.lock();
    Assert.assertFalse(mWriteLock.tryLock());
  }

  /**
   * Tests the {@link ClientRWLock#lockInterruptibly()} method.
   */
  @Test
  public void lockInterruptiblyTest() throws Exception {
    mReadLock.lockInterruptibly();
    Assert.assertTrue(true);
  }
}
