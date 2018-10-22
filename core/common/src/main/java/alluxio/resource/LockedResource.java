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
import java.util.concurrent.locks.Lock;

/**
 * A resource guarded by a lock. Useful in the pattern:
 *
 * <pre>
 * try (LockedResource<T> resource = acquireResource()) {
 *   doSomething(resource.get());
 * }
 * </pre>
 *
 * This is useful when returning a resource that has been locked. It indicates to the caller that
 * they need to unlock the resource, usually by using try-with-resources.
 *
 * @param <T> the type of object guarded by the resource
 */
public class LockedResource<T> implements Closeable {
  private final T mObject;
  private final Lock mLock;

  /**
   * @param object the locked object
   * @param lock the lock that is locking the object; this will be unlocked when this LockedResource
   *        is closed
   */
  public LockedResource(T object, Lock lock) {
    mObject = object;
    mLock = lock;
  }

  /**
   * @return the internal object
   */
  public T get() {
    return mObject;
  }

  @Override
  public void close() {
    mLock.unlock();
  }
}
