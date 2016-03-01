/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.resource;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Unit test for {@code ResourcePool} class.
 */
public class ResourcePoolTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  class TestResourcePool extends ResourcePool<Integer> {
    int mPort = 0;

    public TestResourcePool(int maxCapacity) {
      super(maxCapacity);
    }

    public TestResourcePool(int maxCapacity, ConcurrentLinkedQueue<Integer> resources) {
      super(maxCapacity, resources);
    }

    @Override
    public void close() {
      // no ops
    }

    @Override
    protected Integer createNewResource() {
      mPort++;
      return Integer.valueOf(mPort);
    }
  }

  /**
   * Tests the normal acquiration of resource pools.
   */
  @Test
  public void resourcePoolNormalTest() {
    TestResourcePool testPool = new TestResourcePool(2);
    int resource1 = testPool.acquire();
    testPool.release(resource1);
    int resource2 = testPool.acquire();
    Assert.assertEquals(resource1, resource2);
  }

  /**
   * Tests that an exception is thrown if the resource pool is used more than its size can take.
   *
   * @throws InterruptedException when the queue starts to poll
   */
  @Test
  public void resourcePoolBlockingTest() throws InterruptedException {
    mThrown.expect(RuntimeException.class);
    final int POOL_SIZE = 2;
    @SuppressWarnings("unchecked")
    ConcurrentLinkedQueue<Integer> queue = Mockito.mock(ConcurrentLinkedQueue.class);
    TestResourcePool testPool = new TestResourcePool(POOL_SIZE, queue);
    Mockito.when(queue.isEmpty()).thenReturn(true);
    Mockito.when(queue.poll()).thenThrow(new InterruptedException());
    for (int i = 0; i < POOL_SIZE + 1; i++) {
      testPool.acquire();
    }
  }
}
