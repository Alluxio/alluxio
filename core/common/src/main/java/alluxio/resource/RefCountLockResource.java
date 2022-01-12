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

import alluxio.concurrent.LockMode;

import com.google.common.base.Preconditions;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakTracker;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;

/**
 * Reference counted Lock resource, automatically unlocks and decrements the reference count.
 * It contains a lock and a reference count for that lock, and will decrement
 * the lock reference count and unlocking when the resource is closed.
 */
public class RefCountLockResource extends RWLockResource {

  private static final ResourceLeakDetector<RefCountLockResource> DETECTOR =
      AlluxioResourceLeakDetectorFactory.instance()
          .newResourceLeakDetector(RefCountLockResource.class);

  private final AtomicInteger mRefCount;

  @Nullable
  private final ResourceLeakTracker<RefCountLockResource> mTracker = DETECTOR.track(this);

  /**
   * Creates a new instance of {@link LockResource} using the given lock and reference counter. The
   * reference counter should have been initialized and incremented outside of this class.
   *
   * @param lock the lock to acquire
   * @param mode the mode to acquire the lock in
   * @param acquireLock whether to lock the lock
   * @param refCount ref count for the lock
   * @param useTryLock applicable only if acquireLock is true. Determines whether or not to use
   *                   {@link Lock#tryLock()} or {@link Lock#lock()} to acquire the lock
   */
  public RefCountLockResource(ReentrantReadWriteLock lock, LockMode mode, boolean acquireLock,
      AtomicInteger refCount, boolean useTryLock) {
    super(lock, mode, acquireLock, useTryLock);
    mRefCount = Preconditions.checkNotNull(refCount,
        "Reference Counter can not be null");
  }

  /**
   * Releases the lock and decrement the ref count if a ref counter was provided
   * at construction time.
   */
  @Override
  public void close() {
    super.close();
    if (mTracker != null) {
      mTracker.close(this);
    }
    mRefCount.decrementAndGet();
  }
}
