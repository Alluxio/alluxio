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

/**
 * Class representing a pool of resources to be temporarily used and returned. Inheriting classes
 * should implement the close method as well as initialize the resources in the constructor. The
 * implemented methods are thread-safe and inheriting classes should also written in a
 * thread-safe manner. See {@link tachyon.client.file.FSMasterClientPool} as an example.
 *
 * @param <T> The type of resource this pool manages.
 */
// TODO: This may fit better in the common module
public abstract class ResourcePool<T> {
  protected final Object mCapacityLock;
  protected final int mMaxCapacity;
  protected final BlockingQueue<T> mResources;
  protected int mCurrentCapacity;

  public ResourcePool(int maxCapacity) {
    mCapacityLock = new Object();
    mMaxCapacity = maxCapacity;
    mResources = new LinkedBlockingQueue<T>(maxCapacity);
  }

  /**
   * Acquires an object of type T, this operation is blocking if no clients are available.
   *
   * @return a MasterClientBase, guaranteed to be only available to the caller
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
      // TODO: Investigate the best way to handle this
      throw new RuntimeException(ie);
    }
  }

  /**
   * Closes the resource pool. After this call, the object should be discarded. Inheriting
   * classes should clean up all their resources here.
   */
  public abstract void close();

  /**
   * Creates a new resource which will be added to the resource pool after the user is done using
   * it. This method should only be called when the capacity of the pool has not been reached. If
   * creating the resource will take a significant amount of time, the inheriting class should
   * avoid calling this method and instead initialize all the resources in the constructor.
   *
   * @return a resource which will be added to the pool of resources.
   */
  public abstract T createNewResource();

  /**
   * Releases an object of type T, this must be called after the thread is done using a resource
   * obtained by acquire.
   *
   * @param resource the resource to be released, it should not be reused after calling this method
   */
  public void release(T resource) {
    mResources.add(resource);
  }
}
