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

package alluxio.concurrent;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link CountingLatch}.
 */
public class CountingLatchTest {
  private static final long SLEEP_MILLIS = 1000;
  private static final long STILL_BLOCKED = -1;

  private CountingLatch mLatch;

  private static class BlockingThread extends Thread {
    private volatile long mBlockedTimeMillis;
    private final Runnable mRunnable;
    private final long mStart;

    public BlockingThread(Runnable runnable) {
      mBlockedTimeMillis = STILL_BLOCKED;
      mRunnable = runnable;
      mStart = System.currentTimeMillis();
    }

    public void run() {
      mRunnable.run();
      mBlockedTimeMillis = System.currentTimeMillis() - mStart;
    }

    public long getBlockedTimeMillis() {
      return mBlockedTimeMillis;
    }
  }

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  @Before
  public void before() {
    mLatch = new CountingLatch();
  }

  /**
   * Tests that inc and dec can proceed without being blocked when there is no await.
   */
  @Test
  public void noAwait() throws Exception {
    final int N = 10;
    for (int i = 0; i < N; i++) {
      Assert.assertEquals(0, mLatch.getState());
      mLatch.inc();
      Assert.assertEquals(1, mLatch.getState());
      mLatch.dec();
      Assert.assertEquals(0, mLatch.getState());
    }
    for (int i = 0; i < N; i++) {
      Assert.assertEquals(i, mLatch.getState());
      mLatch.inc();
    }
    for (int i = 0; i < N; i++) {
      mLatch.dec();
      Assert.assertEquals(N - 1 - i, mLatch.getState());
    }
  }

  /**
   * Tests that inc is blocked when await returns,
   * and inc is unblocked when release returns.
   */
  @Test
  public void blockAndUnblockInc() throws Exception {
    Assert.assertEquals(0, mLatch.getState());
    mLatch.await();
    Assert.assertEquals(-1, mLatch.getState());

    BlockingThread inc = new BlockingThread(() -> {
      try {
        mLatch.inc();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    inc.start();

    Thread.sleep(SLEEP_MILLIS);
    mLatch.release();

    inc.join();
    Assert.assertTrue(
        String.format("BlockedTimeMillis: %s", inc.getBlockedTimeMillis()),
        inc.getBlockedTimeMillis() >= SLEEP_MILLIS);
    Assert.assertEquals(1, mLatch.getState());
  }

  /**
   * Tests that multiple blocked inc can be unblocked when release returns.
   */
  @Test
  public void unblockMultipleInc() throws Exception {
    Assert.assertEquals(0, mLatch.getState());
    mLatch.await();
    Assert.assertEquals(-1, mLatch.getState());

    List<BlockingThread> incThreads = new ArrayList<>();
    final int numThreads = 10;
    for (int i = 0; i < numThreads; i++) {
      incThreads.add(new BlockingThread(() -> {
        try {
          mLatch.inc();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }));
    }
    for (BlockingThread t : incThreads) {
      t.start();
    }

    Thread.sleep(SLEEP_MILLIS);
    mLatch.release();

    for (BlockingThread t : incThreads) {
      t.join();
      Assert.assertTrue(
          String.format("BlockedTimeMillis: %s", t.getBlockedTimeMillis()),
          t.getBlockedTimeMillis() >= SLEEP_MILLIS);
    }
    Assert.assertEquals(numThreads, mLatch.getState());
  }

  /**
   * Tests that await is blocked when state is not 0.
   */
  @Test
  public void await() throws Exception {
    Assert.assertEquals(0, mLatch.getState());
    mLatch.inc();
    Assert.assertEquals(1, mLatch.getState());
    mLatch.inc();
    Assert.assertEquals(2, mLatch.getState());

    BlockingThread await = new BlockingThread(mLatch::await);
    await.start();

    Assert.assertEquals(STILL_BLOCKED, await.getBlockedTimeMillis());

    Thread.sleep(SLEEP_MILLIS);
    mLatch.dec();
    Assert.assertEquals(1, mLatch.getState());
    Assert.assertEquals(STILL_BLOCKED, await.getBlockedTimeMillis());

    Thread.sleep(SLEEP_MILLIS);
    mLatch.dec();
    Assert.assertEquals(0, mLatch.getState());

    await.join();
    Assert.assertTrue(String.format("BlockedTimeMillis: %s", await.getBlockedTimeMillis()),
        await.getBlockedTimeMillis() >= 2 * SLEEP_MILLIS);
    Assert.assertEquals(-1, mLatch.getState());
  }

  /**
   * Tests that when await is blocked, further inc will not be blocked.
   * If the assumption is wrong, this test will be blocked and timed out.
   */
  @Test
  public void blockedAwait() throws Exception {
    Assert.assertEquals(0, mLatch.getState());
    mLatch.inc();
    Assert.assertEquals(1, mLatch.getState());

    BlockingThread await = new BlockingThread(mLatch::await);
    await.start();

    int incAfterAwait = 10;
    for (int i = 0; i < incAfterAwait; i++) {
      mLatch.inc();
      Assert.assertEquals(i + 2, mLatch.getState());
    }
    for (int i = 0; i < incAfterAwait; i++) {
      mLatch.dec();
      Assert.assertEquals(incAfterAwait - i, mLatch.getState());
    }

    mLatch.dec();
    await.join();
    Assert.assertEquals(-1, mLatch.getState());
  }

  /**
   * Tests that calling dec without a paired inc will throw exception.
   */
  @Test
  public void decWithoutInc() throws Exception {
    mLatch.inc();
    mLatch.dec();
    mExpectedException.expect(Error.class);
    mLatch.dec();
  }

  /**
   * Tests that release without a paired await will throw exception.
   */
  @Test
  public void releaseWithoutAwait() {
    mLatch.await();
    mLatch.release();
    mExpectedException.expect(Error.class);
    mLatch.release();
  }

  /**
   * Tests that inc can be interrupted.
   */
  @Test
  public void interruptInc() throws Exception {
    mLatch.await();
    Thread inc = new Thread(() -> {
      try {
        mLatch.inc();
      } catch (InterruptedException e) {
        // Expected.
      }
    });
    inc.start();
    inc.join(SLEEP_MILLIS);
    Assert.assertEquals(Thread.State.WAITING, inc.getState());
    inc.interrupt();
    inc.join();
  }
}
