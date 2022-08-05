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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.util.ThreadFactoryUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class SharedResourcePoolTest {
  private static final class TestResource implements Closeable, SharableResource {
    int mResourceId;
    boolean mClosed;

    private TestResource(int id) {
      mResourceId = id;
      mClosed = false;
    }

    @Override
    public void close() {
      mClosed = true;
    }
  }

  private static final class SharedTestResourcePool extends SharedResourcePool<TestResource> {

    // Monotonically increasing resource id generator
    private int mNextId = 0;

    // Fail to create after some number of creations,
    // used to simulation error scenarios
    // -1 if never fails
    private final int mFailAfterNumCreate;

    private int mNumCreate = 0;

    /**
     * Create a new resource queue.
     *
     * @param options construction options
     */
    public SharedTestResourcePool(DynamicResourcePool.Options options, int failAfterNumCreate) {
      super(options);
      mFailAfterNumCreate = failAfterNumCreate;
    }

    @Override
    protected TestResource createNewResource() throws IOException {
      if (mNumCreate == mFailAfterNumCreate) {
        throw new IOException();
      }
      mNumCreate += 1;
      int id = mNextId;
      mNextId++;
      return new TestResource(id);
    }
  }

  // max capacity in test cases
  private static final int MAX_CAPACITY = 100;

  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(5,
          ThreadFactoryUtils.build("TestPool-%d", true));

  // set gc delay high so that it will not be triggered.
  private static final DynamicResourcePool.Options DEFAULT_OPTIONS =
      DynamicResourcePool.Options.defaultOptions()
          .setGcExecutor(GC_EXECUTOR).setInitialDelayMs(100000).setMaxCapacity(MAX_CAPACITY);

  private SharedResourcePool<TestResource> mResourcePool;

  @Before
  public void before() {
    mResourcePool = new SharedTestResourcePool(DEFAULT_OPTIONS, -1);
  }

  @After
  public void after() throws Exception {
    mResourcePool.close();
  }

  @Test
  public void acquireUnderMaxCapacity() throws Exception {
    // should all succeed and return a different resource
    for (int i = 0; i < MAX_CAPACITY; ++i) {
      CloseableResource<TestResource> resource = mResourcePool.acquire();
      assertEquals(i, resource.get().mResourceId);
      assertEquals(i + 1, mResourcePool.size());
    }
  }

  @Test
  public void acquireExceedMaxCapacity() throws Exception {
    // maps resourceId to numberOfReference
    // #keys should <= MAX_CAPACITY, sum of values should equal total
    // number of acquire calls
    Map<Integer, Integer> resourceIdToRefCount = new HashMap<>();

    int numAcquireCalls = 2 * MAX_CAPACITY;
    for (int i = 0; i < numAcquireCalls; ++i) {
      // should all succeed without blocking
      CloseableResource<TestResource> resource = mResourcePool.acquire();

      int resourceId = resource.get().mResourceId;
      // should not create more than MAX_CAPACITY resources
      assertTrue(resourceId < MAX_CAPACITY);
      resourceIdToRefCount.compute(resourceId, (key, old) -> old == null ? 1 : old + 1);
    }

    // we should not allocate more resources than MAX_CAPACITY
    assertTrue(resourceIdToRefCount.size() <= MAX_CAPACITY);
    assertEquals(numAcquireCalls,
        resourceIdToRefCount.values().stream().mapToInt(Integer::intValue).sum());
  }

  @Test
  public void gcInternal() throws Exception {
    List<CloseableResource<TestResource>> resourceList = new ArrayList<>();
    for (int i = 0; i < MAX_CAPACITY; i++) {
      CloseableResource<TestResource> resource = mResourcePool.acquire();
      assertFalse(resource.get().mClosed);
      resourceList.add(resource);
    }

    // this gc should do nothing as all resources are being held
    mResourcePool.gcUnusedResources(false);
    for (CloseableResource<TestResource> ref: resourceList) {
      assertFalse(ref.get().mClosed);
    }

    // now we close all references manually, which should make next gc collect all
    // the resources
    for (CloseableResource<TestResource> ref: resourceList) {
      ref.close();
    }
    mResourcePool.gcUnusedResources(false);

    // now all the resource should be closed
    for (CloseableResource<TestResource> ref: resourceList) {
      assertTrue(ref.get().mClosed);
    }
    assertEquals(0, mResourcePool.size());
  }

  @Test
  public void concurrentAcquireAndClose() throws Exception {
    ConcurrentMap<Integer, Integer> resourceIdToRefCount = new ConcurrentHashMap<>();

    int numAcquireCallsPerThread = 50;
    int numThread = 10;
    Thread[] threads = new Thread[10];
    for (int i = 0; i < numThread; i++) {
      Thread thread = new Thread(() -> {
        for (int j = 0; j < numAcquireCallsPerThread; j++) {
          try (CloseableResource<TestResource> resource = mResourcePool.acquire()) {
            // resource should be working
            assertFalse(resource.get().mClosed);
            // keep track of resource id
            int id = resource.get().mResourceId;
            assertTrue(id <= MAX_CAPACITY);
            resourceIdToRefCount.compute(id, (key, old) -> old == null ? 1 : old + 1);
          } catch (IOException e) {
            // nothing to do
          }
        }
      });
      thread.start();
      threads[i] = thread;
    }

    for (Thread t: threads) {
      // all threads should succeed
      t.join();
    }

    // first sanity check: we allocated no more than MAX_CAPACITY resources
    // and the ref count matches up
    assertTrue(resourceIdToRefCount.size() <= MAX_CAPACITY);
    assertEquals(numAcquireCallsPerThread * numThread,
        resourceIdToRefCount.values().stream().mapToInt(Integer::intValue).sum());

    // second sanity check: after all the concurrent threads closed their resource, now
    // all the resource should have reference count 0 and should be garbage collected in the
    // next gc round
    mResourcePool.gcUnusedResources(false);
    assertEquals(0, mResourcePool.size());
  }

  @Test
  public void failCreateFallback() throws Exception {
    // first create will succeed, second will fail
    mResourcePool = new SharedTestResourcePool(DEFAULT_OPTIONS, 1);
    CloseableResource<TestResource> firstResource = mResourcePool.acquire();
    CloseableResource<TestResource> secondResource = mResourcePool.acquire();

    // should fall back to shared resource
    assertEquals(firstResource.get(), secondResource.get());
  }

  @Test
  public void failCreateErrOut() throws Exception {
    // first create will fail
    mResourcePool = new SharedTestResourcePool(DEFAULT_OPTIONS, 0);
    assertThrows(IOException.class, () -> mResourcePool.acquire());
  }

  @Test
  public void reuseResourceWhenPossible() throws Exception {
    TestResource shouldBeReused;
    try (CloseableResource<TestResource> first = mResourcePool.acquire()) {
      shouldBeReused = first.get();
      assertFalse(shouldBeReused.mClosed);
    }

    // since we released the last reference, we should reuse existing
    // resource
    try (CloseableResource<TestResource> second = mResourcePool.acquire()) {
      assertSame(shouldBeReused, second.get());
    }
  }
}
