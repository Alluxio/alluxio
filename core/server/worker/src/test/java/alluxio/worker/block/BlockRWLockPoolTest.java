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

package alluxio.worker.block;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

public class BlockRWLockPoolTest {
  /**
   * To test the number of remaining resources in the pool. We report this value as a metric.
   * pool size of 10, 10 threads acquire locks and release early
   * while another 10 threads acquire locks but release late.
   * in the first checkpoint, the remaining resource should be 0,
   * in the second checkpoint,
   * the remaining resource should be the size of the pool as we returned all locks.
   */
  @Test
  public void testMetricReporting() throws BrokenBarrierException, InterruptedException {
    final int numIterations = 10;
    final int numLocksEarlyRelease = 10;
    final int numLocksLateRelease = 10;
    final int poolCapacity = 10;
    Random r = new Random();
    for (int i = 0; i < numIterations; ++i) {
      BlockRWLockPool pool = new BlockRWLockPool(poolCapacity);
      List<Thread> threads = new ArrayList<>();
      final CyclicBarrier barrier1 =
          new CyclicBarrier(numLocksEarlyRelease + numLocksLateRelease + 1);
      final CyclicBarrier barrier2 = new CyclicBarrier(numLocksLateRelease + 1);
      for (int j = 0; j < numLocksEarlyRelease; ++j) {
        threads.add(new Thread(
            () -> {
              try {
                Thread.sleep(r.nextInt(50));
                ClientRWLock l = pool.acquire();
                Thread.sleep(r.nextInt(100));
                pool.release(l);
                barrier1.await();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
        ));
      }
      for (int j = 0; j < numLocksLateRelease; ++j) {
        threads.add(new Thread(
            () -> {
              try {
                Thread.sleep(100);
                Thread.sleep(r.nextInt(100));
                ClientRWLock l = pool.acquire();
                Thread.sleep(r.nextInt(100));
                barrier1.await();
                barrier2.await();
                pool.release(l);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
        ));
      }
      threads.forEach(Thread::start);
      barrier1.await();
      Assert.assertEquals(0, pool.mRemainingPoolResources.get());
      barrier2.await();
      threads.forEach((it) -> {
        try {
          it.join();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      Assert.assertEquals(poolCapacity, pool.mRemainingPoolResources.get());
    }
  }

  /**
   * To test acquire and timed acquire and
   * make sure the lock are acquired and released as expected.
   */
  @Test
  public void testAcquire() {
    BlockRWLockPool pool = new BlockRWLockPool(1);

    ClientRWLock lock1 = pool.acquire(1, TimeUnit.SECONDS);
    Assert.assertEquals(0, pool.mRemainingPoolResources.get());
    new Thread(() -> {
      try {
        Thread.sleep(3000);
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
      pool.release(lock1);
    }).start();
    Assert.assertNotNull(lock1);

    ClientRWLock lock2 = pool.acquire(100, TimeUnit.MILLISECONDS);
    Assert.assertNull(lock2);
    Assert.assertEquals(0, pool.mRemainingPoolResources.get());

    ClientRWLock lock3 = pool.acquire();
    Assert.assertNotNull(lock3);
    Assert.assertEquals(0, pool.mRemainingPoolResources.get());
    pool.release(lock3);
    Assert.assertEquals(1, pool.mRemainingPoolResources.get());
  }
}
