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

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Class representing a pool of resources to be temporarily used and returned. Inheriting classes
 * must implement the close method as well as initialize the resources in the constructor. The
 * implemented methods are thread-safe and inheriting classes should also written in a thread-safe
 * manner. See {@code FileSystemMasterClientPool} as an example.
 *
 * @param <T> the type of resource this pool manages
 */
@ThreadSafe
public abstract class ResourcePool<T> implements Pool<T> {
  private static final long WAIT_INDEFINITELY = -1;
  private final ReentrantLock mTakeLock;
  private final Condition mNotEmpty;
  protected final int mMaxCapacity;
  protected final ConcurrentLinkedQueue<T> mResources;
  /** It represents the total number of resources that have been created by this pool. */
  protected final AtomicInteger mCurrentCapacity;

  /**
   * Creates a {@link ResourcePool} instance with the specified capacity.
   *
   * @param maxCapacity the maximum of resources in this pool
   */
  public ResourcePool(int maxCapacity) {
    this(maxCapacity, new ConcurrentLinkedQueue<T>());
  }

  /**
   * Internal constructor that can provide an object to be used for the internal queue.
   *
   * @param maxCapacity bhe maximum of resources in this pool
   * @param resources blocking queue to use
   */
  protected ResourcePool(int maxCapacity, ConcurrentLinkedQueue<T> resources) {
    mTakeLock = new ReentrantLock();
    mNotEmpty = mTakeLock.newCondition();
    mMaxCapacity = maxCapacity;
    mCurrentCapacity = new AtomicInteger();
    mResources = resources;
  }

  /**
   * Acquires an object of type {@code T} from the pool. This operation is blocking if no resource
   * is available. Each call of {@link #acquire()} should be paired with another call of
   * {@link #release(Object)}} after the use of this resource completes to return this resource to
   * the pool.
   *
   * @return a resource taken from the pool
   */
  @Override
  public T acquire() {
    return acquire(WAIT_INDEFINITELY, null);
  }

  /**
   * Acquires an object of type {code T} from the pool.
   *
   * This method is like {@link #acquire()}, but it will time out if an object cannot be
   * acquired before the specified amount of time.
   *
   * @param time an amount of time to wait, <= 0 to wait indefinitely
   * @param unit the unit to use for time, null to wait indefinitely
   * @return a resource taken from the pool, or null if the operation times out
   */
  @Override
  @Nullable
  public T acquire(long time, TimeUnit unit) {
    // If either time or unit are null, the other should also be null.
    Preconditions.checkState((time <= 0) == (unit == null));
    long endTimeMs = 0;
    if (time > 0) {
      endTimeMs = System.currentTimeMillis() + unit.toMillis(time);
    }

    // Try to take a resource without blocking
    T resource = mResources.poll();
    if (resource != null) {
      return resource;
    }

    if (mCurrentCapacity.getAndIncrement() < mMaxCapacity) {
      // If the resource pool is empty but capacity is not yet full, create a new resource.
      return createNewResource();
    }

    mCurrentCapacity.decrementAndGet();
    // Otherwise, try to take a resource from the pool, blocking if none are available.
    try {
      mTakeLock.lockInterruptibly();
      try {
        while (true) {
          resource = mResources.poll();
          if (resource != null) {
            return resource;
          }
          if (time > 0) {
            long currTimeMs = System.currentTimeMillis();
            // one should use t1-t0<0, not t1<t0, because of the possibility of numerical overflow.
            // For further detail see: https://docs.oracle.com/javase/8/docs/api/java/lang/System.html
            if (endTimeMs - currTimeMs <= 0) {
              return null;
            }
            if (!mNotEmpty.await(endTimeMs - currTimeMs, TimeUnit.MILLISECONDS)) {
              return null;
            }
          } else {
            mNotEmpty.await();
          }
        }
      } finally {
        mTakeLock.unlock();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Closes the resource pool. After this call, the object should be discarded. Inheriting classes
   * should clean up all their resources here.
   */
  @Override
  public abstract void close() throws IOException;

  /**
   * Releases an object of type T, this must be called after the thread is done using a resource
   * obtained by acquire.
   *
   * @param resource the resource to be released, it should not be reused after calling this method
   */
  @Override
  public void release(T resource) {
    if (resource != null) {
      mResources.add(resource);
      try (LockResource r = new LockResource(mTakeLock)) {
        mNotEmpty.signal();
      }
    }
  }

  @Override
  public int size() {
    return mCurrentCapacity.get();
  }

  /**
   * Creates a new resource which will be added to the resource pool after the user is done using
   * it. This method should only be called when the capacity of the pool has not been reached. If
   * creating the resource will take a significant amount of time, the inheriting class should
   * avoid calling this method and instead initialize all the resources in the constructor.
   *
   * @return a resource which will be added to the pool of resources
   */
  public abstract T createNewResource();
}
