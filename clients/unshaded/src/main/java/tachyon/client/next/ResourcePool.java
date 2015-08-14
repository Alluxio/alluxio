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

package tachyon.client.next;

import java.util.concurrent.BlockingQueue;

/**
 * Class representing a pool of resources to be temporarily used and returned. Inheriting classes
 * should implement the close method as well as initialize the resources in the constructor.
 *
 * @param <T> The type of resource this pool manages.
 */
public abstract class ResourcePool<T> {
  protected BlockingQueue<T> mResources;

  /**
   * Acquires an object of type T, this operation is blocking if no clients are available.
   *
   * @return a MasterClient, guaranteed to be only available to the caller
   */
  public T acquire() {
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
   * Releases an object of type T, this must be called after the thread is done using a resource
   * obtained by acquire.
   *
   * @param resource the resource to be released, it should not be reused after calling this method
   */
  public void release(T resource) {
    mResources.add(resource);
  }
}
