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

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakTracker;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import javax.annotation.Nullable;

/**
 * A resource lock that makes it possible to acquire and release locks using the following idiom:
 *
 * <pre>
 *   try (LockResource r = new LockResource(lock)) {
 *     ...
 *   }
 * </pre>
 *
 * The subclasses will be tracked by the leak detector.
 * The subclasses should call super.close() in their close(), otherwise a leak will be reported.
 */
// extends Closeable instead of AutoCloseable to enable usage with Guava's Closer.
public class LockResource implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(LockResource.class);

  private static final ResourceLeakDetector<LockResource> DETECTOR =
      AlluxioResourceLeakDetectorFactory.instance().newResourceLeakDetector(LockResource.class);

  // The lock which represents the resource. It should only be written or modified by subclasses
  // attempting to downgrade locks (see RWLockResource).
  protected Lock mLock;
  private final Runnable mCloseAction;
  @Nullable
  private ResourceLeakTracker<LockResource> mTracker = DETECTOR.track(this);

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * @param lock the lock to acquire
   */
  public LockResource(Lock lock) {
    this(lock, true, false);
  }

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * This method may use the {@link Lock#tryLock()} method to gain ownership of the locks. The
   * reason one might want to use this is to avoid the fairness heuristics within the
   * {@link java.util.concurrent.locks.ReentrantReadWriteLock}'s NonFairSync which may block reader
   * threads if a writer is the first in the queue.
   *
   * @param lock the lock to acquire
   * @param acquireLock whether to lock the lock
   * @param useTryLock whether or not use to {@link Lock#tryLock()}
   */
  public LockResource(Lock lock, boolean acquireLock, boolean useTryLock) {
    this(lock, acquireLock, useTryLock, null);
  }

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * This method may use the {@link Lock#tryLock()} method to gain ownership of the locks. The
   * reason one might want to use this is to avoid the fairness heuristics within the
   * {@link java.util.concurrent.locks.ReentrantReadWriteLock}'s NonFairSync which may block reader
   * threads if a writer is the first in the queue.
   *
   * @param lock the lock to acquire
   * @param acquireLock whether to lock the lock
   * @param useTryLock whether or not use to {@link Lock#tryLock()}
   * @param closeAction the nullable closeable that will be run before releasing the lock
   */
  public LockResource(Lock lock, boolean acquireLock, boolean useTryLock,
      @Nullable Runnable closeAction) {
    mLock = lock;
    mCloseAction = closeAction;
    if (acquireLock) {
      if (useTryLock) {
        while (!mLock.tryLock()) { // returns immediately
          // The reason we don't use #tryLock(int, TimeUnit) here is because we found there is a bug
          // somewhere in the internal accounting of the ReentrantRWLock that, even though all
          // threads had released the lock, that a final thread would never be able to acquire it.
          LockSupport.parkNanos(10000);
        }
      } else {
        mLock.lock();
      }
    }
  }

  /**
   * Returns true if the other {@link LockResource} contains the same lock.
   *
   * @param other other LockResource
   * @return true if the other lockResource has the same lock
   */
  @VisibleForTesting
  public boolean hasSameLock(LockResource other) {
    return mLock == other.mLock;
  }

  /**
   * @return the value of {@link ObjectUtils#identityToString}
   * for the underlying lock object
   */
  @VisibleForTesting
  public String getLockIdentity() {
    return ObjectUtils.identityToString(mLock);
  }

  /**
   * Releases the lock.
   */
  @Override
  public void close() {
    if (mCloseAction != null) {
      mCloseAction.run();
    }
    if (mTracker != null) {
      mTracker.close(this);
    }
    mLock.unlock();
  }
}
