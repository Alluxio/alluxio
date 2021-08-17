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

package alluxio.util.executor;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Tests the {@link UniqueBlockingQueue}.
 */
public class UniqueBlockingQueueTest {
  @Test
  public void uniqueness() throws Exception {
    UniqueBlockingQueue<Integer> test = new UniqueBlockingQueue<>(4);
    assertEquals(0, test.size());
    test.put(1);
    test.put(1);
    assertEquals(1, test.size());
    test.put(2);
    assertEquals(2, test.size());
    assertEquals(1, test.poll().intValue());
    assertEquals(2, test.poll().intValue());
  }

  @Test
  public void concurrentTest() throws Exception {
    UniqueBlockingQueue<Integer> test = new UniqueBlockingQueue<>(100);
    ExecutorService executor = Executors.newFixedThreadPool(5);
    for (int i = 0; i < 5; i++) {
      executor.submit(() -> {
        for (int j = 0; j < 20; j++) {
          try {
            test.put(j);
          } catch (InterruptedException e) {
            // do nothing
          }
        }
      });
    }
    executor.shutdown();
    assertEquals(20, test.size());
  }
}
