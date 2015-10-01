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

package tachyon.resource;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

/**
 * Unit test for <code>ResourcePool</code> class.
 */
public class ResourcePoolTest {
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  class TestResourcePool extends ResourcePool<Integer> {
    int mPort = 0;

    public TestResourcePool(int maxCapacity) {
      super(maxCapacity);
    }

    public TestResourcePool(int maxCapacity, BlockingQueue<Integer> resources) {
      super(maxCapacity, resources);
    }

    @Override
    public void close() {
      // no ops
    }

    @Override
    protected Integer createNewResource() {
      mPort ++;
      return Integer.valueOf(mPort);
    }
  }

  @Test
  public void resourcePoolNormalTest() {
    TestResourcePool testPool = new TestResourcePool(2);
    int resource1 = testPool.acquire();
    testPool.release(resource1);
    int resource2 = testPool.acquire();
    Assert.assertEquals(resource1, resource2);
  }

  @Test
  public void resourcePoolBlockingTest() throws InterruptedException {
    mThrown.expect(RuntimeException.class);
    LinkedBlockingQueue queue = PowerMockito.mock(LinkedBlockingQueue.class);
    TestResourcePool testPool = new TestResourcePool(2, queue);
    Mockito.when(queue.isEmpty()).thenReturn(true);
    Mockito.when(queue.take()).thenThrow(new InterruptedException());
    int resource1 = testPool.acquire();
    int resource2 = testPool.acquire();
    int resource3 = testPool.acquire();
  }
}
