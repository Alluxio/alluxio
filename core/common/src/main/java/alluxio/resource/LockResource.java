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

import java.util.concurrent.locks.Lock;

/**
 * A resource lock that makes it possible to acquire and release locks using the following idiom:
 *
 * <pre>
 *   try (LockResource r = new LockResource(lock)) {
 *     ...
 *   }
 * </pre>
 */
public class LockResource implements AutoCloseable {
  private final Lock mLock;

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * @param lock the lock to acquire
   */
  public LockResource(Lock lock) {
    mLock = lock;
    mLock.lock();
  }

  /**
   * Releases the lock.
   */
  @Override
  public void close() {
    mLock.unlock();
  }
}
