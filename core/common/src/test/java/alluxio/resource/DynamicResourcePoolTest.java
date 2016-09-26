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

import alluxio.Constants;
import alluxio.clock.ManualClock;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class DynamicResourcePoolTest {
  private static final class Resource {
    private Integer mInteger = 0;
    // Threshold for invalid resource.
    private static final int INVALID_RESOURCE = 10;

    public Resource(Integer i) {
      mInteger = i;
    }

    public Resource setInteger(Integer i) {
      mInteger = i;
      return this;
    }
  }

  private static final class TestPool extends DynamicResourcePool<Resource> {
    private int mGcThresholdInSecs = 120;
    private int mCounter = 0;

    public TestPool(Options options, ManualClock clock) {
      super(options);
      mClock = clock;
    }

    public TestPool(Options options) {
      super(options);
    }

    @Override
    protected boolean shouldGc(ResourceInternal<Resource> resourceInternal) {
      return mClock.millis() - resourceInternal.getLastAccessTimeMs()
          >= (long) mGcThresholdInSecs * (long) Constants.SECOND_MS;
    }

    @Override
    protected boolean isHealthy(Resource resource) {
      return resource.mInteger < Resource.INVALID_RESOURCE;
    }

    @Override
    protected void closeResource(Resource resource) {
      resource.setInteger(Resource.INVALID_RESOURCE);
    }

    @Override
    protected void closeResourceSync(Resource resource) {
      closeResource(resource);
    }

    @Override
    protected Resource createNewResource() {
      return new Resource(mCounter++);
    }

    public void setGcThresholdInSecs(int gcThresholdInSecs) {
      mGcThresholdInSecs = gcThresholdInSecs;
    }
  }

  /**
   * Tests the logic to acquire a resource when the pool is not full.
   */
  @Test
  public void acquireWithCapacity() throws Exception {
    TestPool pool = new TestPool(DynamicResourcePool.Options.defaultOptions());
    List<Resource> resourceList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Resource resource = pool.acquire();
      resourceList.add(resource);
      Assert.assertEquals(i, resource.mInteger.intValue());
    }

    for (Resource resource : resourceList) {
      pool.release(resource);
    }

    Set<Integer> resources = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      Resource resource = pool.acquire();
      resources.add(resource.mInteger);
    }
    // Make sure we are not creating new resources.
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(resources.contains(i));
    }
  }

  /**
   * Acquire without capacity.
   */
  @Test
  public void acquireWithoutCapacity() throws Exception {
    TestPool pool = new TestPool(DynamicResourcePool.Options.defaultOptions().setMaxCapacity(1));
    List<Resource> resourceList = new ArrayList<>();
    boolean timeout = false;
    try {
      Resource resource = pool.acquire();
      resourceList.add(resource);
      Assert.assertEquals(0, resource.mInteger.intValue());

      resource = pool.acquire(1, TimeUnit.SECONDS);
      resourceList.add(resource);
    } catch (TimeoutException e) {
      timeout = true;
    }
    Assert.assertEquals(1, resourceList.size());
    Assert.assertTrue(timeout);
  }

  /**
   * Tests the logic that invalid resource won't be acquired.
   */
  @Test
  public void UnHealhyResource() throws Exception {
    TestPool pool = new TestPool(DynamicResourcePool.Options.defaultOptions());
    Resource resource = pool.acquire();
    Assert.assertEquals(0, resource.mInteger.intValue());
    resource.setInteger(Resource.INVALID_RESOURCE);

    pool.release(resource);
    resource = pool.acquire();

    // The 0-th resource is not acquired because it is unhealthy.
    Assert.assertEquals(1, resource.mInteger.intValue());
  }

  /**
   * Tests the logic that the recently used resource is preferred.
   */
  @Test
  public void acquireRentlyUsed() throws Exception {
    ManualClock manualClock = new ManualClock();
    TestPool pool = new TestPool(DynamicResourcePool.Options.defaultOptions(), manualClock);
    List<Resource> resourceList = new ArrayList<>();
    resourceList.add(pool.acquire());
    resourceList.add(pool.acquire());
    resourceList.add(pool.acquire());

    pool.release(resourceList.get(2));
    pool.release(resourceList.get(0));

    manualClock.addTimeMs(1500);
    pool.release(resourceList.get(1));

    for (int i = 0; i < 10; i++) {
      Resource resource = pool.acquire();
      Assert.assertEquals(1, resource.mInteger.intValue());
      pool.release(resource);
    }
  }

  @Test
  public void gc() throws Exception {
    ManualClock manualClock = new ManualClock();
    TestPool pool = new TestPool(
        DynamicResourcePool.Options.defaultOptions().setGcIntervalMs(10).setInitialDelayMs(1),
        manualClock);
    pool.setGcThresholdInSecs(1);

    List<Resource> resourceList = new ArrayList<>();
    resourceList.add(pool.acquire());
    resourceList.add(pool.acquire());

    pool.release(resourceList.get(0));
    manualClock.addTimeMs(1001);

    // Sleep 1 second to make sure the GC has run.
    Thread.sleep(1000);

    // Resource 0 is gc-ed.
    Assert.assertEquals(2, pool.acquire().mInteger.intValue());
  }

  @Test
  public void multiClients() throws Exception {
    TestPool pool = new TestPool(DynamicResourcePool.Options.defaultOptions().setMaxCapacity(1));
    final Resource resource1 = pool.acquire();
    Assert.assertEquals(0, resource1.mInteger.intValue());

    class ReleaseThread extends Thread {
      private TestPool mPool;
      private Resource mResource;

      ReleaseThread(TestPool pool, Resource resource) {
        mPool = pool;
        mResource = resource;
      }

      @Override
      public void run() {
        try {
          // Sleep sometime to test wait logic.
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          return;
        }
        mPool.release(mResource);
      }
    }

    ReleaseThread releaseThread = new ReleaseThread(pool, resource1);
    releaseThread.start();
    Resource resource2 = pool.acquire(2, TimeUnit.SECONDS);
    Assert.assertEquals(0, resource2.mInteger.intValue());
  }
}
