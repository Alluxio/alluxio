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

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;
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
// extends Closeable instead of AutoCloseable to enable usage with Guava's Closer.
public class LockResource implements Closeable {
  private final Lock mLock;
  private final AtomicInteger mRefCount;

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * @param lock the lock to acquire
   */
  public LockResource(Lock lock) {
    mLock = lock;
    mLock.lock();
    mRefCount = null;
  }

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * @param lock the lock to acquire
   * @param refCount ref count for the lock
   */
  public LockResource(Lock lock, AtomicInteger refCount) {
    mLock = lock;
    mLock.lock();
    mRefCount = refCount;
  }

  /**
   * Releases the lock and decrement the ref count if a ref counter was provided
   * at construction time.
   */
  @Override
  public void close() {
    mLock.unlock();
    if (mRefCount != null) {
      mRefCount.decrementAndGet();
    }
  }
}
