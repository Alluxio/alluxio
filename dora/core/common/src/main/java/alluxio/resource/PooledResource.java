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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;

/**
 * A pooled resource that was acquired from a {@link Pool}, and will be released back
 * to the pool when it's closed. If the resource is leaked from the pool, the resource will
 * be closed if it's closeable.
 *
 * @param <T> resource type
 */
public class PooledResource<T> extends CloseableResource<T> {
  private static final Logger LOG = LoggerFactory.getLogger(PooledResource.class);
  // weak reference is important as a leaked resource shouldn't keep the entire pool alive
  protected final WeakReference<Pool<T>> mPool;
  protected final String mPoolDescription;

  /**
   * Creates a new pooled resource with the pool from which it was acquired.
   *
   * @param resource the resource
   * @param pool the pool where the resource was acquired
   */
  public PooledResource(T resource, Pool<T> pool) {
    super(resource);
    mPool = new WeakReference<>(pool);
    mPoolDescription = String.format("%s@%s",
        pool.getClass().getName(), Integer.toHexString(pool.hashCode())).intern();
  }

  @Override
  public void closeResource() {
    Pool<T> pool = mPool.get();
    if (pool != null) {
      pool.release(get());
    } else {
      // the pool is gone before this resource can be released, report a leak
      T leaked = get();
      String resType = leaked.getClass().getName();
      LOG.warn("resource of type {} leaked from pool {} which had been GCed before the resource "
          + "could be released", resType, mPoolDescription);
      // do a best effort attempt to close the resource
      if (leaked instanceof AutoCloseable) {
        try {
          ((AutoCloseable) leaked).close();
        } catch (Exception e) {
          throw new RuntimeException(
              String.format("failed to close leaked resource %s: %s", resType, e), e);
        }
      }
    }
  }
}
