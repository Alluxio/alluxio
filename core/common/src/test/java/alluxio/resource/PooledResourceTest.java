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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PooledResourceTest {
  @Test
  public void releaseOnClose() {
    TestPool pool = new TestPool(5);
    try (PooledResource<Integer> i = new PooledResource<>(pool.acquire(), pool)) {
      assertEquals(1, (int) i.get());
      assertEquals(0, pool.getAvailableCount());
      assertEquals(1, pool.getLeakCount());
    }
    assertEquals(1, pool.getAvailableCount());
    assertEquals(0, pool.getLeakCount());
  }

  @Test
  public void leakDoesNotPreventPoolFromBeingGCed() throws Exception {
    long gcTimeout = 1000;
    TestPool pool = new TestPool(5);
    ReferenceQueue<TestPool> referenceQueue = new ReferenceQueue<>();
    PhantomReference<TestPool> poolRef = new PhantomReference<>(pool, referenceQueue);
    PooledResource<Integer> i = new PooledResource<>(pool.acquire(), pool);
    PooledResource<Integer> toLeak = new PooledResource<>(pool.acquire(), pool);
    assertEquals(1, (int) i.get());
    assertEquals(2, (int) toLeak.get());
    assertEquals(0, pool.getAvailableCount());
    assertEquals(2, pool.getLeakCount());

    i.close();
    assertEquals(1, pool.getLeakCount());
    assertEquals(1, pool.getAvailableCount());

    // GC the pool before toLeak is released
    pool = null;
    System.gc();

    // check the pool has really been GCed
    Reference<? extends TestPool> enqueued = referenceQueue.remove(gcTimeout);
    assertNotNull("ref is null, pool is likely not GCed before timeout", enqueued);
    assertEquals(poolRef, enqueued);
    enqueued.clear();

    // we should still be able to close this without getting an exception
    toLeak.close();
  }

  static class TestPool extends ResourcePool<Integer> {
    int mCounter = 0;

    TestPool(int capacity) {
      super(capacity, new ConcurrentLinkedQueue<>());
    }

    @Override
    public void close() throws IOException {
      // do nothing
    }

    @Override
    public Integer createNewResource() {
      mCounter += 1;
      return mCounter;
    }

    /**
     * Gets the number of resources that are immediately available in the pool, without having to
     * call {@link #createNewResource()}.
     *
     * @return number of resources
     */
    public int getAvailableCount() {
      return mResources.size();
    }

    /**
     * Gets the number of resources that has been acquired from this pooled but not released yet.
     *
     * @return the number of resources that is considered leaked
     */
    public int getLeakCount() {
      return size() - getAvailableCount();
    }
  }
}
