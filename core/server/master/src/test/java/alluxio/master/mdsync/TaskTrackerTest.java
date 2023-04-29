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

package alluxio.master.mdsync;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.file.options.DescendantType;
import alluxio.master.file.meta.SyncCheck;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class TaskTrackerTest {

  ExecutorService mThreadPool;
  TaskTracker mTaskTracker;
  MdSync mMdSync;
  MockUfsClient mUfsClient;
  UfsSyncPathCache mUfsSyncPathCache;
  SyncProcess mSyncProcess;
  UfsStatus mFileStatus = new UfsFileStatus("file", "",
      0L, 0L, "", "", (short) 0, 0L);
  UfsStatus mDirStatus = new UfsDirectoryStatus("dir", "", "", (short) 0);
  static final long WAIT_TIMEOUT = 5_000;

  @Before
  public void before() {
    mThreadPool = Executors.newCachedThreadPool();
    mUfsClient = Mockito.spy(new MockUfsClient());
    mSyncProcess = Mockito.spy(new SyncProcess());
    mUfsSyncPathCache = Mockito.mock(UfsSyncPathCache.class);
    mTaskTracker = new TaskTracker(
        1, 1, false, false,
        mUfsSyncPathCache, mSyncProcess);
    mMdSync = new MdSync(mTaskTracker, a -> a, a -> mUfsClient);
  }

  @After
  public void after() throws Exception {
    assertFalse(mTaskTracker.hasRunningTasks());
    mTaskTracker.close();
    mThreadPool.shutdown();
  }

  void checkStats(
      TaskStats stats, int batches, int statuses, int loadErrors,
      int loadRequests, boolean loadFailed, boolean processFailed) {
    if (batches >= 0) {
      assertEquals(batches, stats.getBatchCount());
    }
    if (statuses >= 0) {
      assertEquals(statuses, stats.getStatusCount());
    }
    if (loadErrors >= 0) {
      assertEquals(loadErrors, stats.getLoadErrors());
    }
    if (loadRequests >= 0) {
      assertEquals(loadRequests, stats.getLoadRequestCount());
    }
    assertEquals(loadFailed, stats.isLoadFailed());
    assertEquals(processFailed, stats.isProcessFailed());
  }

  @Test
  public void concurrentProcessTest() throws Throwable {
    // Be sure ufs loads, and result processing can happen concurrently
    mTaskTracker.close();
    int concurrentUfsLoads = 5;
    int totalBatches = 100;
    int concurrentProcessing = 5;
    mTaskTracker = new TaskTracker(
        concurrentProcessing, concurrentUfsLoads, false, false,
        mUfsSyncPathCache, mSyncProcess);
    mMdSync = new MdSync(mTaskTracker, a -> a, a -> mUfsClient);
    AtomicInteger remainingLoadCount = new AtomicInteger(totalBatches);
    AtomicInteger processingCount = new AtomicInteger(0);
    mUfsClient.setResultFunc(path -> {
      int nxtItem = remainingLoadCount.decrementAndGet();
      boolean truncated = nxtItem != 0;
      return new Pair<>(Stream.of(mFileStatus), truncated);
    });
    Mockito.doReturn(SyncCheck.shouldSyncWithTime(0))
        .when(mUfsSyncPathCache).shouldSyncPath(any(), anyLong(), any());

    for (int i = 0; i < 100; i++) {
      remainingLoadCount.set(totalBatches);
      processingCount.set(0);
      CountDownLatch blocker = new CountDownLatch(1);
      Mockito.doAnswer(ans -> {
        processingCount.incrementAndGet();
        // block the processing to ensure we have concurrent load requests
        blocker.await();
        return ans.callRealMethod();
      }).when(mSyncProcess).performSync(any(), any());
      Future<Pair<Boolean, BaseTask>> task = mThreadPool.submit(() ->
          mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
              DescendantType.ALL, 0, DirectoryLoadType.NONE));
      CommonUtils.waitForResult("Concurrent load", remainingLoadCount::get,
          v -> v == totalBatches - concurrentUfsLoads - concurrentProcessing,
          WaitForOptions.defaults().setTimeoutMs(1000));
      CommonUtils.waitForResult("Concurrent processing", processingCount::get,
          v -> v == concurrentProcessing,
          WaitForOptions.defaults().setTimeoutMs(1000));
      // let the processing complete
      blocker.countDown();
      Pair<Boolean, BaseTask> result = task.get();
      assertTrue(result.getFirst());
      result.getSecond().waitComplete(WAIT_TIMEOUT);
      assertEquals(remainingLoadCount.get(), 0);
      TaskStats stats = result.getSecond().getTaskInfo().getStats();
      checkStats(stats, 100, 100, 0, 100, false, false);
    }
  }

  @Test
  public void concurrentDirProcessErrorTest() throws Throwable {
    // Fail processing during concurrent ufs loading and processing when using load by directory
    mTaskTracker.close();
    int concurrentUfsLoads = 5;
    int totalBatches = 100;
    int processError = 95;
    int concurrentProcessing = 5;
    AtomicInteger remainingProcessCount = new AtomicInteger(processError);
    mTaskTracker = new TaskTracker(
        concurrentProcessing, concurrentUfsLoads, false, false,
        mUfsSyncPathCache, mSyncProcess);
    Mockito.doAnswer(ans -> {
      if (remainingProcessCount.decrementAndGet() == 0) {
        throw new IOException();
      }
      return ans.callRealMethod();
    }).when(mSyncProcess).performSync(any(), any());
    Mockito.doReturn(SyncCheck.shouldSyncWithTime(0))
        .when(mUfsSyncPathCache).shouldSyncPath(any(), anyLong(), any());

    mMdSync = new MdSync(mTaskTracker, a -> a, a -> mUfsClient);
    for (int i = 0; i < 100; i++) {
      for (DirectoryLoadType loadType
          : ImmutableList.of(DirectoryLoadType.DFS, DirectoryLoadType.BFS)) {
        AtomicInteger remainingLoadCount = new AtomicInteger(totalBatches);
        remainingProcessCount.set(processError);
        mUfsClient.setResultFunc(path -> {
          int nxtItem = remainingLoadCount.decrementAndGet();
          boolean truncated = nxtItem > 0;
          return new Pair<>(Stream.of(mFileStatus, mDirStatus), truncated);
        });

        Future<Pair<Boolean, BaseTask>> task = mThreadPool.submit(() ->
            mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
                DescendantType.ALL, 0, loadType));
        Pair<Boolean, BaseTask> result = task.get();
        assertFalse(result.getFirst());
        result.getSecond().waitComplete(WAIT_TIMEOUT);
        TaskStats stats = result.getSecond().getTaskInfo().getStats();
        checkStats(stats, -1, -1, -1, -1, false, true);
      }
    }
  }

  @Test
  public void concurrentDirLoadErrorTest() throws Throwable {
    // Fail processing during concurrent ufs loading and processing
    mTaskTracker.close();
    int concurrentUfsLoads = 5;
    int totalBatches = 100;
    int concurrentProcessing = 5;
    mTaskTracker = new TaskTracker(
        concurrentProcessing, concurrentUfsLoads, false, false,
        mUfsSyncPathCache, mSyncProcess);
    Mockito.doReturn(SyncCheck.shouldSyncWithTime(0))
        .when(mUfsSyncPathCache).shouldSyncPath(any(), anyLong(), any());
    AtomicInteger remainingLoadCount = new AtomicInteger(totalBatches);
    mUfsClient.setResultFunc(path -> {
      int nxtItem = remainingLoadCount.decrementAndGet();
      boolean truncated = nxtItem > 0;
      if (truncated) {
        return new Pair<>(Stream.of(mFileStatus, mDirStatus), true);
      } else {
        throw new RuntimeException();
      }
    });

    mMdSync = new MdSync(mTaskTracker, a -> a, a -> mUfsClient);
    for (int i = 0; i < 100; i++) {
      for (DirectoryLoadType loadType
          : ImmutableList.of(DirectoryLoadType.DFS, DirectoryLoadType.BFS)) {
        remainingLoadCount.set(totalBatches);
        Future<Pair<Boolean, BaseTask>> task = mThreadPool.submit(() ->
            mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
                DescendantType.ALL, 0, loadType));
        Pair<Boolean, BaseTask> result = task.get();
        assertFalse(result.getFirst());
        result.getSecond().waitComplete(WAIT_TIMEOUT);
        TaskStats stats = result.getSecond().getTaskInfo().getStats();
        checkStats(stats, -1, -1, -1, -1, true, false);
      }
    }
  }

  @Test
  public void concurrentDirLoadTest() throws Throwable {
    // Fail processing during concurrent ufs loading and processing
    mTaskTracker.close();
    int concurrentUfsLoads = 5;
    int totalBatches = 100;
    int concurrentProcessing = 5;
    mTaskTracker = new TaskTracker(
        concurrentProcessing, concurrentUfsLoads, false, false,
        mUfsSyncPathCache, mSyncProcess);
    mMdSync = new MdSync(mTaskTracker, a -> a, a -> mUfsClient);
    Mockito.doReturn(SyncCheck.shouldSyncWithTime(0))
        .when(mUfsSyncPathCache).shouldSyncPath(any(), anyLong(), any());
    AtomicInteger remainingLoadCount = new AtomicInteger(totalBatches);
    mUfsClient.setResultFunc(path -> {
      int nxtItem = remainingLoadCount.decrementAndGet();
      boolean truncated = nxtItem > 0;
      if (truncated) {
        return new Pair<>(Stream.of(mFileStatus, mDirStatus), true);
      } else {
        return new Pair<>(Stream.of(mFileStatus), false);
      }
    });

    for (int i = 0; i < 100; i++) {
      for (DirectoryLoadType loadType
          : ImmutableList.of(DirectoryLoadType.DFS, DirectoryLoadType.BFS)) {
        remainingLoadCount.set(totalBatches);

        Future<Pair<Boolean, BaseTask>> task = mThreadPool.submit(() ->
            mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
                DescendantType.ALL, 0, loadType));
        Pair<Boolean, BaseTask> result = task.get();
        assertTrue(result.getFirst());
        result.getSecond().waitComplete(WAIT_TIMEOUT);
        TaskStats stats = result.getSecond().getTaskInfo().getStats();
        checkStats(stats, -1, -1, 0, -1, false, false);
      }
    }
  }

  @Test
  public void concurrentProcessErrorTest() throws Throwable {
    // Fail processing during concurrent ufs loading and processing
    mTaskTracker.close();
    int concurrentUfsLoads = 5;
    int totalBatches = 100;
    int batchFailureNumber = 50;
    int concurrentProcessing = 5;
    mTaskTracker = new TaskTracker(
        concurrentProcessing, concurrentUfsLoads, false, false,
        mUfsSyncPathCache, mSyncProcess);
    mMdSync = new MdSync(mTaskTracker, a -> a, a -> mUfsClient);
    AtomicInteger remainingLoadCount = new AtomicInteger(totalBatches);
    AtomicInteger processingCount = new AtomicInteger(0);
    mUfsClient.setResultFunc(path -> {
      int nxtItem = remainingLoadCount.decrementAndGet();
      boolean truncated = nxtItem != 0;
      return new Pair<>(Stream.of(mFileStatus), truncated);
    });
    Mockito.doAnswer(ans -> {
      if (processingCount.incrementAndGet() == batchFailureNumber) {
        throw new IOException();
      }
      return ans.callRealMethod();
    }).when(mSyncProcess).performSync(any(), any());
    Mockito.doReturn(SyncCheck.shouldSyncWithTime(0))
        .when(mUfsSyncPathCache).shouldSyncPath(any(), anyLong(), any());

    for (int i = 0; i < 100; i++) {
      remainingLoadCount.set(totalBatches);
      processingCount.set(0);
      Future<Pair<Boolean, BaseTask>> task = mThreadPool.submit(() ->
          mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
              DescendantType.ALL, 0, DirectoryLoadType.NONE));
      Pair<Boolean, BaseTask> result = task.get();
      assertFalse(result.getFirst());
      result.getSecond().waitComplete(WAIT_TIMEOUT);
      TaskStats stats = result.getSecond().getTaskInfo().getStats();
      checkStats(stats, -1, -1, 0, -1, false, true);
    }
  }

  @Test
  public void concurrentLoadErrorTest() throws Throwable {
    // Fail processing during concurrent ufs loading and processing
    mTaskTracker.close();
    int concurrentUfsLoads = 5;
    int totalBatches = 100;
    int loadFailNumber = 50;
    int concurrentProcessing = 5;
    mTaskTracker = new TaskTracker(
        concurrentProcessing, concurrentUfsLoads, false, false,
        mUfsSyncPathCache, mSyncProcess);
    mMdSync = new MdSync(mTaskTracker, a -> a, a -> mUfsClient);
    AtomicInteger remainingLoadCount = new AtomicInteger(totalBatches);
    mUfsClient.setResultFunc(path -> {
      int nxtItem = remainingLoadCount.decrementAndGet();
      if (nxtItem <= loadFailNumber) {
        throw new RuntimeException();
      }
      return new Pair<>(Stream.of(mFileStatus), true);
    });
    Mockito.doReturn(SyncCheck.shouldSyncWithTime(0))
        .when(mUfsSyncPathCache).shouldSyncPath(any(), anyLong(), any());

    for (int i = 0; i < 100; i++) {
      remainingLoadCount.set(totalBatches);
      Future<Pair<Boolean, BaseTask>> task = mThreadPool.submit(() ->
          mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
              DescendantType.ALL, 0, DirectoryLoadType.NONE));
      Pair<Boolean, BaseTask> result = task.get();
      assertFalse(result.getFirst());
      result.getSecond().waitComplete(WAIT_TIMEOUT);
      TaskStats stats = result.getSecond().getTaskInfo().getStats();
      checkStats(stats, -1, -1, 4, -1, true, false);
    }
  }

  @Test
  public void concurrentLoadTest() throws Throwable {
    // be sure loads can happen concurrently
    mTaskTracker.close();
    int concurrentUfsLoads = 5;
    int totalBatches = 100;
    mTaskTracker = new TaskTracker(
        1, concurrentUfsLoads, false, false,
        mUfsSyncPathCache, mSyncProcess);
    mMdSync = new MdSync(mTaskTracker, a -> a, a -> mUfsClient);
    AtomicInteger count = new AtomicInteger(totalBatches);
    mUfsClient.setResultFunc(path -> {
      int nxtItem = count.decrementAndGet();
      boolean truncated = nxtItem != 0;
      return new Pair<>(Stream.of(mFileStatus), truncated);
    });
    Mockito.doReturn(SyncCheck.shouldSyncWithTime(0))
        .when(mUfsSyncPathCache).shouldSyncPath(any(), anyLong(), any());

    for (int i = 0; i < 100; i++) {
      count.set(totalBatches);
      CountDownLatch blocker = new CountDownLatch(1);
      Mockito.doAnswer(ans -> {
        // block the processing to ensure we have concurrent load requests
        blocker.await();
        return ans.callRealMethod();
      }).when(mSyncProcess).performSync(any(), any());

      Future<Pair<Boolean, BaseTask>> task = mThreadPool.submit(() ->
          mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
              DescendantType.ALL, 0, DirectoryLoadType.NONE));
      CommonUtils.waitForResult("Concurrent load", count::get,
          v -> v == totalBatches - concurrentUfsLoads - 1,
          WaitForOptions.defaults().setTimeoutMs(1000));
      // let the processing complete
      blocker.countDown();
      Pair<Boolean, BaseTask> result = task.get();
      assertTrue(result.getFirst());
      result.getSecond().waitComplete(WAIT_TIMEOUT);
      assertEquals(count.get(), 0);
      TaskStats stats = result.getSecond().getTaskInfo().getStats();
      checkStats(stats, 100, 100, 0, 100, false, false);
    }
  }

  @Test
  public void dirLoadTest() throws Exception {
    // Load nested directories one level at a time in different batch requests
    mUfsClient.setResultFunc(path -> {
      if (path.equals("/")) {
        return new Pair<>(Stream.of(mFileStatus, mDirStatus), false);
      } else if (path.equals("/dir")) {
        return new Pair<>(Stream.of(mFileStatus, mFileStatus), false);
      } else {
        throw new RuntimeException("should not reach");
      }
    });

    for (int i = 0; i < 100; i++) {
      // Use load type BFS, there should be a load task for both / and /dir
      Mockito.doReturn(SyncCheck.shouldSyncWithTime(0))
          .when(mUfsSyncPathCache).shouldSyncPath(any(), anyLong(), any());
      Pair<Boolean, BaseTask> result = mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
          DescendantType.ALL, 0, DirectoryLoadType.BFS);
      assertTrue(result.getFirst());
      result.getSecond().waitComplete(WAIT_TIMEOUT);
      TaskStats stats = result.getSecond().getTaskInfo().getStats();
      checkStats(stats, 2, 4, 0, 2, false, false);

      // run the same request, except have the sync for the nested directory not be needed
      Mockito.doReturn(SyncCheck.shouldNotSyncWithTime(0))
          .when(mUfsSyncPathCache).shouldSyncPath(any(), anyLong(), any());
      result = mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
          DescendantType.ALL, 0, DirectoryLoadType.BFS);
      assertTrue(result.getFirst());
      result.getSecond().waitComplete(WAIT_TIMEOUT);
      stats = result.getSecond().getTaskInfo().getStats();
      checkStats(stats, 1, 2, 0, 1, false, false);
    }
  }

  @Test
  public void basicSyncTest() throws Exception {
    for (int i = 0; i < 100; i++) {
      mUfsClient.setResult(Collections.singletonList(Stream.of(mFileStatus)).iterator());
      Pair<Boolean, BaseTask> result = mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
          DescendantType.NONE, 0, DirectoryLoadType.NONE);
      assertTrue(result.getFirst());
      result.getSecond().waitComplete(WAIT_TIMEOUT);
      TaskStats stats = result.getSecond().getTaskInfo().getStats();
      checkStats(stats, 1, 1, 0, 1, false, false);
    }
  }

  @Test
  public void multiBatchTest() throws Exception {
    // load a directory of 2 batches of size 1
    for (int i = 0; i < 100; i++) {
      mUfsClient.setResult(ImmutableList.of(Stream.of(mFileStatus),
          Stream.of(mFileStatus)).iterator());
      Pair<Boolean, BaseTask> result = mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
          DescendantType.NONE, 0, DirectoryLoadType.NONE);
      assertTrue(result.getFirst());
      result.getSecond().waitComplete(WAIT_TIMEOUT);
      TaskStats stats = result.getSecond().getTaskInfo().getStats();
      checkStats(stats, 2, 2, 0, 2, false, false);
    }
  }

  @Test
  public void loadErrorTest() throws Exception {
    // Ufs loads return errors until failure
    for (int i = 0; i < 100; i++) {
      mUfsClient.setError(new Throwable());
      Pair<Boolean, BaseTask> result = mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
          DescendantType.NONE, 0, DirectoryLoadType.NONE);
      assertFalse(result.getFirst());
      result.getSecond().waitComplete(WAIT_TIMEOUT);
      TaskStats stats = result.getSecond().getTaskInfo().getStats();
      checkStats(stats, 0, 0, 4, 1, true, false);
    }
  }

  @Test
  public void loadErrorRetryTest() throws Exception {
    int totalBatches = 100;
    // Error on the first load, but let the next succeed
    AtomicInteger count = new AtomicInteger(totalBatches);
    mUfsClient.setResultFunc(path -> {
      int nxtItem = count.decrementAndGet();
      boolean truncated = nxtItem != 0;
      if (truncated && nxtItem % 2 ==  0) {
        throw new RuntimeException();
      }
      return new Pair<>(Stream.of(mFileStatus), truncated);
    });
    for (int i = 0; i < 100; i++) {
      count.set(totalBatches);
      Pair<Boolean, BaseTask> result = mTaskTracker.checkTask(mMdSync,
          new AlluxioURI("/"), DescendantType.NONE, 0,
          DirectoryLoadType.NONE);
      assertTrue(result.getFirst());
      result.getSecond().waitComplete(WAIT_TIMEOUT);
      TaskStats stats = result.getSecond().getTaskInfo().getStats();
      int amount = totalBatches / 2;
      checkStats(stats, amount + 1, amount + 1, amount - 1, amount + 1, false, false);
    }
  }

  @Test
  public void processErrorTest() throws Throwable {
    // An error happens during processing
    for (int i = 0; i < 100; i++) {
      mUfsClient.setResult(ImmutableList.of(Stream.of(mFileStatus),
          Stream.of(mFileStatus)).iterator());
      Mockito.doThrow(new IOException()).when(mSyncProcess).performSync(any(), any());
      Pair<Boolean, BaseTask> result = mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
          DescendantType.NONE, 0, DirectoryLoadType.NONE);
      assertFalse(result.getFirst());
      result.getSecond().waitComplete(WAIT_TIMEOUT);
      TaskStats stats = result.getSecond().getTaskInfo().getStats();
      checkStats(stats, -1, -1, 0, 2, false, true);
    }
  }

  @Test
  public void blockingSyncTest() throws Throwable {
    // run two concurrent processing syncing on the same path
    // be sure one is blocked and they both succeed
    for (int i = 0; i < 2; i++) {
      mUfsClient.setResult(Collections.singletonList(Stream.of(mFileStatus)).iterator());
      Semaphore blocker = new Semaphore(0);
      Mockito.doAnswer(ans -> {
        // block the processing of any task
        blocker.acquire();
        return ans.callRealMethod();
      }).when(mSyncProcess).performSync(any(), any());
      // Submit two concurrent tasks on the same path
      Future<Pair<Boolean, BaseTask>> task1 = mThreadPool.submit(() ->
          mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
              DescendantType.NONE, 0, DirectoryLoadType.NONE));
      assertThrows(TimeoutException.class, () -> task1.get(1, TimeUnit.SECONDS));
      Future<Pair<Boolean, BaseTask>> task2 = mThreadPool.submit(() ->
          mTaskTracker.checkTask(mMdSync, new AlluxioURI("/"),
              DescendantType.NONE, 0, DirectoryLoadType.NONE));
      assertThrows(TimeoutException.class, () -> task2.get(1, TimeUnit.SECONDS));
      // Let one task be processed
      blocker.release();
      // Only one task should have been executed, but both should finish since they
      // were on the same path
      assertTrue(task1.get().getFirst());
      assertTrue(task2.get().getFirst());
      TaskStats stats1 = task1.get().getSecond().getTaskInfo().getStats();
      checkStats(stats1, 1, 1, 0, 1, false, false);
      TaskStats stats2 = task2.get().getSecond().getTaskInfo().getStats();
      checkStats(stats2, 1, 1, 0, 1, false, false);
    }
  }
}
