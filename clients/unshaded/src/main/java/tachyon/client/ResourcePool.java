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

package tachyon.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Preconditions;

/**
 * Class representing a pool of resources to be temporarily used and returned. Inheriting classes
 * must implement the close method as well as initialize the resources in the constructor. The
 * implemented methods are thread-safe and inheriting classes should also written in a thread-safe
 * manner. See {@link tachyon.client.file.FSMasterClientPool} as an example.
 *
 * @param <T> The type of resource this pool manages.
 */
// TODO(calvin): This may fit better in the common module.
public abstract class ResourcePool<T> {
  protected final Object mCapacityLock;
  protected final int mMaxCapacity;
  protected final BlockingQueue<T> mResources;
  protected int mCurrentCapacity;

  /**
   * Creates a {@link ResourcePool} instance with the specified capacity.
   *
   * @param maxCapacity the maximum of resources in this pool
   */
  public ResourcePool(int maxCapacity) {
    Preconditions.checkArgument(maxCapacity > 0, "Capacity must be non-negative");
    mCapacityLock = new Object();
    mMaxCapacity = maxCapacity;
    mCurrentCapacity = 0;
    mResources = new LinkedBlockingQueue<T>(maxCapacity);
  }

  /**
   * Acquires an object of type {@code T} from the pool. This operation is blocking if no resource
   * is available. Each call of {@link #acquire} should be paired with another call of
   * {@link #release} after the use of this resource completes to return this resource to the pool.
   *
   * @return a resource taken from the pool
   */
  public T acquire() {
    // If the resource pool is empty but capacity is not yet full, create a new resource.
    synchronized (mCapacityLock) {
      if (mResources.isEmpty() && mCurrentCapacity < mMaxCapacity) {
        T newResource = createNewResource();
        mCurrentCapacity ++;
        return newResource;
      }
    }

    // Otherwise, try to take a resource from the pool, blocking if none are available.
    try {
      return mResources.take();
    } catch (InterruptedException ie) {
      // TODO(calvin): Investigate the best way to handle this.
      throw new RuntimeException(ie);
    }
  }

  /**
   * Closes the resource pool. After this call, the object should be discarded. Inheriting classes
   * should clean up all their resources here.
   */
  public abstract void close();

  /**
   * Releases an object of type T, this must be called after the thread is done using a resource
   * obtained by acquire.
   *
   * @param resource the resource to be released, it should not be reused after calling this method
   */
  public void release(T resource) {
    mResources.add(resource);
  }

  /**
   * Creates a new resource which will be added to the resource pool after the user is done using
   * it. This method should only be called when the capacity of the pool has not been reached. If
   * creating the resource will take a significant amount of time, the inheriting class should
   * avoid calling this method and instead initialize all the resources in the constructor.
   *
   * @return a resource which will be added to the pool of resources.
   */
  protected abstract T createNewResource();
}
