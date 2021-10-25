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

import static org.junit.Assert.assertTrue;

import alluxio.TestLoggerRule;
import alluxio.collections.LockPool;
import alluxio.concurrent.LockMode;
import alluxio.conf.PropertyKey;

import io.netty.util.ResourceLeakDetector;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AlluxioLeakDetectorTest {

  @Rule
  public TestLoggerRule mLogger = new TestLoggerRule();

  @BeforeClass
  public static void before() {
    System.setProperty(PropertyKey.LEAK_DETECTOR_LEVEL.getName(),
        ResourceLeakDetector.Level.SIMPLE.toString());
  }

  /**
   * This test attempts to generate a leak detector log message by creating a number of locks in
   * a lock pool. We need to create at least 128 in order to have a good chance that we track a
   * single leaked lock.
   *
   * After which, we allocate a bunch of memory and do garbage operations in order to trigger a GC.
   * After triggering a GC, we need to allocate a few more locks again in order to trigger the
   * {@code reportLeaks} method internal to the leak detector.
   */
  @Test
  @Ignore
  public void testLockPool() {
    LockPool<Long> pool = new LockPool<>((key) -> new ReentrantReadWriteLock(),
        0, 900, 1000, 10);
    for (int i = 0; i < 200; i++) {
      getAndDiscardRef(pool, i);
    }
    System.gc();
    for (int i = 0; i < 10; i++) {
      byte[] mem = new byte[1024 * 1024 * 1024];
      if (mem[0] == 0x7a) {
        continue;
      }
      mem[ThreadLocalRandom.current().nextInt(1024 * 1024)] += 1;
    }
    for (int i = 200; i < 400; i++) {
      getAndDiscardRef(pool, i);
    }
    System.gc();
    assertTrue(
        mLogger.wasLogged("close\\(\\) was not called before resource is garbage-collected"));
  }

  int getAndDiscardRef(LockPool<Long> pool, long l) {
    LockResource r = pool.get(l, LockMode.WRITE);
    return ThreadLocalRandom.current().nextInt(100);
  }

  @Test
  @Ignore
  public void testCloseableResource() {
    for (int i = 0; i < 10000; i++) {
      createNotCloseIter();
    }
    for (int i = 0; i < 10; i++) {
      byte[] mem = new byte[1024 * 1024 * 1024];
      if (mem[0] == 0x7a) {
        continue;
      }
      mem[ThreadLocalRandom.current().nextInt(1024 * 1024)] += 1;
    }
    System.gc();
    for (int i = 0; i < 10000; i++) {
      createNotCloseIter();
    }
    System.gc();
    assertTrue(
        mLogger.wasLogged("close\\(\\) was not called before resource is garbage-collected"));
  }

  void createNotCloseIter() {
    CloseableIterator.noopCloseable(Arrays.stream(new int[1024]).iterator()).get().next();
  }
}
