package alluxio.worker.block;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class BlockRWLockPoolTest {
    /**
     * To test the number of remaining resources in the pool. We report this value as a metric.
     * pool size of 10, 10 threads acquire locks and release early while another 10 threads acquire locks but release late.
     * in the first checkpoint, the remaining resource should be 0,
     * in the second checkpoint, the remaining resource should be the size of the pool as we returned all locks.
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
            final CyclicBarrier barrier1 = new CyclicBarrier(numLocksEarlyRelease + numLocksLateRelease + 1);
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
            Assert.assertEquals(0, pool.getRemainingPoolResources());
            barrier2.await();
            threads.forEach((it) -> {
                try {
                    it.join();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Assert.assertEquals(poolCapacity, pool.getRemainingPoolResources());
        }
    }

    /**
     * To test logging when the pool is exhausted.
     * pool size of 10, 10 threads acquire locks and release early while another 10 threads acquire locks but release late.
     * Since there must be a point where the pool is exhausted, mHasReachedFullCapacity must have been set true.
     * And hence the message is guaranteed to be logged.
     */
    @Test
    public void testLogging() {
        final int numIterations = 10;
        final int numLocksEarlyRelease = 10;
        final int numLocksLateRelease = 10;
        final int poolCapacity = 10;
        Random r = new Random();
        for (int i = 0; i < numIterations; ++i) {
            BlockRWLockPool pool = new BlockRWLockPool(poolCapacity);
            List<Thread> threads = new ArrayList<>();
            final CyclicBarrier barrier = new CyclicBarrier(numLocksEarlyRelease + numLocksLateRelease);
            for (int j = 0; j < numLocksEarlyRelease; ++j) {
                threads.add(new Thread(
                        () -> {
                            try {
                                Thread.sleep(r.nextInt(50));
                                ClientRWLock l = pool.acquire();
                                Thread.sleep(r.nextInt(100));
                                pool.release(l);
                                barrier.await();
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
                                // Sleep 100ms to make sure early release locks acquire lock first.
                                Thread.sleep(100);
                                Thread.sleep(r.nextInt(100));
                                ClientRWLock l = pool.acquire();
                                barrier.await();
                                Thread.sleep(r.nextInt(100));
                                pool.release(l);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                ));
            }
            threads.forEach(Thread::start);
            threads.forEach((it) -> {
                try {
                    it.join();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Assert.assertTrue(pool.mHasReachedFullCapacity.get());
        }
    }

}
