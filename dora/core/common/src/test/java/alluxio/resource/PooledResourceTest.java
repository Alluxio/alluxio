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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
    try (PooledResource<Resource> i = new PooledResource<>(pool.acquire(), pool)) {
      assertFalse(i.get().isClosed());
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
    PooledResource<Resource> notLeaked = new PooledResource<>(pool.acquire(), pool);
    PooledResource<Resource> leaked = new PooledResource<>(pool.acquire(), pool);
    assertFalse(notLeaked.get().isClosed());
    assertFalse(leaked.get().isClosed());
    assertEquals(0, pool.getAvailableCount());
    assertEquals(2, pool.getLeakCount());

    notLeaked.close();
    // we don't assert on the closed-ness of the resource wrapped in notLeaked here,
    // since it's released to the pool, and the pool will decide when to close it
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
    Resource leakedResource = leaked.get();
    leaked.close();
    assertTrue(leakedResource.isClosed());
  }

  static class Resource implements AutoCloseable {
    private boolean mIsClosed = false;

    @Override
    public void close() throws Exception {
      mIsClosed = true;
    }

    public boolean isClosed() {
      return mIsClosed;
    }
  }

  static class TestPool extends ResourcePool<Resource> {
    TestPool(int capacity) {
      super(capacity, new ConcurrentLinkedQueue<>());
    }

    @Override
    public void close() throws IOException {
      // do nothing
    }

    @Override
    public Resource createNewResource() {
      return new Resource();
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
