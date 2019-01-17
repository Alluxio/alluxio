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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

/**
 * Reference counted Lock resource, automatically unlocks and decrements the reference count.
 */
public class RefCountLockResource extends LockResource {
  private final AtomicInteger mRefCount;

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * @param lock the lock to acquire
   * @param refCount ref count for the lock
   */
  public RefCountLockResource(Lock lock, AtomicInteger refCount) {
    super(lock);
    mRefCount = refCount;
  }

  /**
   * Releases the lock and decrement the ref count if a ref counter was provided
   * at construction time.
   */
  @Override
  public void close() {
    super.close();
    if (mRefCount != null) {
      mRefCount.decrementAndGet();
    }
  }
}
