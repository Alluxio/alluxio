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
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class implements a SharedResourcePool. It manages resources under a different
 * model than an exclusive {@link Pool}: in a pool, resources are owned exclusively,
 * and the workflow is: user acquires a resource from the pool, owns it and works on it
 * exclusively for a time, and then return it back to the pool so that others can acquire it.
 * <p></p>
 * A shared pool manages {@link SharableResource}, i.e., a pool of resources shared among all
 * users. When users acquire a resource from queue, they might get a new one, or they might get
 * a used one to share with other users. The idea is that the underlying resources can be safely
 * shared so that acquire never needs to wait or fail.
 * <p></p>
 * {@link SharedResourcePool} implements the shared pool model by maintaining a priority queue
 * of resources ordered by current reference count.
 *
 * @param <T> The inner resource type. It has to implement {@link SharableResource}, see the docs
 *           there for the requirements of this interface.
 */
@ThreadSafe
public abstract class SharedResourcePool<T extends Closeable & SharableResource>
    implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SharedResourcePool.class);

  /** Max number of resources the pool can hold. */
  private final int mMaxCapacity;

  /** Min number of resources to avoid the pool being too cold. */
  private final int mMinCapacity;

  /** Lock protects internal mutation of resource tree. */
  private final ReentrantLock mLock = new ReentrantLock();

  /** Close flag. */
  private boolean mClosed = false;

  /**
   * Resources are wrapped in a {@link CountingReference} to keep track of its holders.
   * All the resources are managed in a priority queue, so that users acquire the
   * least referenced resource if possible.
   */
  private final PriorityQueue<CountingReference<T>> mResources =
      new PriorityQueue<>((o1, o2) -> {
        // note: this is only partial order, but it works fine with
        // priority queue
        if (o1.equals(o2)) {
          return 0;
        }
        if (o1.getRefCount() <= o2.getRefCount()) {
          return -1;
        }
        return 1;
      });

  /** GC executor that closes unused resources periodically. */
  private final ScheduledExecutorService mExecutor;

  /** GC task future. */
  private final ScheduledFuture<?> mGcFuture;

  /**
   * Create a new resource queue.
   * @param options construction options
   */
  public SharedResourcePool(DynamicResourcePool.Options options) {
    mExecutor = Preconditions.checkNotNull(options.getGcExecutor());
    mMaxCapacity = options.getMaxCapacity();
    mMinCapacity = options.getMinCapacity();
    mGcFuture = mExecutor.scheduleAtFixedRate(
        () -> gcUnusedResources(true),
        options.getInitialDelayMs(),
        options.getGcIntervalMs(),
        TimeUnit.MILLISECONDS);
  }

  /**
   * Acquire a {@link CloseableResource} from the queue, intending
   * not to share.
   *
   * @return a closable resource
   * @throws IOException when the resource pool is empty whereas we failed to create a new one
   */
  public CloseableResource<T> acquire() throws IOException {
    return acquire(0);
  }

  /**
   * Acquire a {@link CloseableResource} from the resource tree.
   * This method accepts a best-effort based `maxAcceptableRefCount` parameter, which
   * follows the following behavior in decreasing priority:
   * 1. There is a resource in the tree that is current referenced less than
   * `maxAcceptableRefCount`, then we reuse the resource.
   * 2. If the current size of the resource pool is less than `maxCapacity`, then we
   * try to create a new resource and use the new one.
   * 3. If the resource pool is full or creating new one fails, then we fall back to sharing
   * the least-referenced resource in the pool.
   * 4. If the resource pool is empty and creating new resource fails, then we throw exception.
   *
   * @param maxAcceptableRefCount the maximum degree of acceptable sharing
   * @return the closeable resource
   * @throws IOException when the resource pool is empty whereas we failed to create a new one
   */
  public CloseableResource<T> acquire(int maxAcceptableRefCount) throws IOException {
    try (LockResource r = new LockResource(mLock)) {
      Preconditions.checkState(!mClosed);

      // firstly try to reuse an eligible resource
      CountingReference<T> ref = mResources.poll();
      if (ref != null
          && (ref.getRefCount() <= maxAcceptableRefCount
              || mResources.size() + 1 >= mMaxCapacity)) {
        ref.reference();
        mResources.add(ref);
        return makeResource(ref);
      } else if (ref != null) {
        mResources.add(ref);
      }

      // secondly try to create a new one
      try {
        CountingReference<T> newResource = new CountingReference<>(createNewResource(), 1);
        mResources.add(newResource);
        return makeResource(newResource);
      } catch (Exception e) {
        if (ref != null) {
          // create new resource failed, but can fall back to use
          // shared resource
          LOG.warn("Reuse shared resource as creating new resource failed", e);
          mResources.remove(ref);
          ref.reference();
          mResources.add(ref);
          return makeResource(ref);
        }
        // can't fall back, error out
        throw new IOException(e);
      }
    }
  }

  /**
   * Wraps the inner {@link CountingReference} in a {@link CloseableResource} for return.
   * @param ref the inner shared reference of the resource
   * @return the closeable resource for public use
   */
  private CloseableResource<T> makeResource(CountingReference<T> ref) {
    return new CloseableResource<T>(ref.get()) {
      @Override
      public void closeResource() {
        try (LockResource r = new LockResource(mLock)) {
          mResources.remove(ref);
          ref.dereference();
          mResources.add(ref);
        }
      }
    };
  }

  /**
   * @return the number of resources the queue currently holds
   */
  public int size() {
    try (LockResource r = new LockResource(mLock)) {
      Preconditions.checkState(!mClosed);

      return mResources.size();
    }
  }

  /**
   * Create new resource.
   * Note that this method could be called inside critical sections when acquiring
   * new resources from the pool. So it is generally advisable to avoid expensive
   * operations here, as that might compromise the performance during contention
   * scenarios.
   *
   * @return newly created resource
   * @throws IOException if error occurs during creation
   */
  protected abstract T createNewResource() throws Exception;

  protected void closeResource(T resource) throws IOException {
    resource.close();
  }

  /**
   * Garbage collects unused resources.
   * @param onlyGcWhenFull if true, then the routine won't do anything if
   *                       the resource pool hasn't reached full capacity,
   *                       also it will abort once the pool has reached min
   *                       capacity.
   *                       Otherwise, it gcs all unused resources.
   */
  @VisibleForTesting
  void gcUnusedResources(boolean onlyGcWhenFull) {
    ArrayList<T> resourceToGc = new ArrayList<>();
    try (LockResource r = new LockResource(mLock)) {
      if (onlyGcWhenFull && mResources.size() < mMaxCapacity) {
        return;
      }

      int curSize = mResources.size();
      Iterator<CountingReference<T>> iterator = mResources.iterator();
      while (iterator.hasNext()) {
        CountingReference<T> nxt = iterator.next();
        if (nxt.getRefCount() == 0) {
          resourceToGc.add(nxt.get());
          iterator.remove();
          curSize -= 1;

          // abort since the pool is cold enough
          if (onlyGcWhenFull && curSize <= mMinCapacity) {
            break;
          }
        }
      }
    }

    for (T resource: resourceToGc) {
      try {
        closeResource(resource);
      } catch (IOException e) {
        LOG.debug("Resource {} failed to be closed: {}", resource, e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try (LockResource r = new LockResource(mLock)) {
      if (mClosed) {
        return;
      }

      mClosed = true;
      int resourceInUse = 0;

      for (CountingReference<T> resource: mResources) {
        if (resource.getRefCount() != 0) {
          resourceInUse++;
        }

        try {
          closeResource(resource.get());
        } catch (IOException e) {
          // log and continue closing other resources
          LOG.warn("Resource {} cannot be closed: {}", resource, e);
        }
      }

      if (resourceInUse != 0) {
        LOG.warn("{} resources are not released when closing the resource queue.", resourceInUse);
      }
      mResources.clear();
      mGcFuture.cancel(true);
    }
  }
}
