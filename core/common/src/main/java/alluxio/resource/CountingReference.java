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

import com.google.common.base.Objects;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A reference counting wrapper around any resource type. It typically works with
 * a ResourcePool to keep track of which resource is owned.
 * @param <T> The wrapped resource type
 */
public class CountingReference<T> {
  private final T mObject;
  private final AtomicInteger mRefCount;

  /**
   * Create counting reference wrapper.
   * @param object the inner resource
   * @param initialRefCount initial reference count
   */
  public CountingReference(T object, int initialRefCount) {
    mObject = object;
    mRefCount = new AtomicInteger(initialRefCount);
  }

  /**
   * @return the underlying object after increasing ref-count
   */
  public CountingReference<T> reference() {
    mRefCount.incrementAndGet();
    return this;
  }

  /**
   * Decrement the ref-count for underlying object.
   *
   * @return the current ref count after dereference
   */
  public int dereference() {
    return mRefCount.decrementAndGet();
  }

  /**
   * @return current ref-count
   */
  public int getRefCount() {
    return mRefCount.get();
  }

  /**
   * @return the underlying object without changing the ref-count
   */
  public T get() {
    return mObject;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mObject, mRefCount);
  }

  @Override
  public boolean equals(Object other) {
    return this == other;
  }
}
