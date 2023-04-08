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

import static alluxio.file.options.DescendantType.ALL;
import static alluxio.master.mdsync.BatchPathWaiterTest.completeFirstLoadRequestEmpty;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;

import alluxio.AlluxioURI;
import alluxio.file.options.DirectoryLoadType;
import alluxio.master.file.metasync.SyncResult;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RunWith(Parameterized.class)
public class DirectoryPathWaiterTest {

  @Parameterized.Parameters
  public static Collection<DirectoryLoadType> directoryLoadTypes() {
    return Arrays.asList(DirectoryLoadType.DFS, DirectoryLoadType.BFS);
  }

  public DirectoryPathWaiterTest(DirectoryLoadType loadType) {
    mDirLoadType = loadType;
  }

  DirectoryLoadType mDirLoadType;
  ExecutorService mThreadPool;
  Clock mClock = Clock.systemUTC();
  MdSync mMdSync;

  @Before
  public void before() {
    mThreadPool = Executors.newCachedThreadPool();
    mMdSync = Mockito.spy(new MdSync(Mockito.mock(TaskTracker.class)));
  }

  @After
  public void after() {
    mThreadPool.shutdown();
  }

  @Test
  public void TestWaiter() throws Exception {
    TaskInfo ti = new TaskInfo(mMdSync, new AlluxioURI("/path"),
        new AlluxioURI("/path"), null,
        ALL, 0, mDirLoadType, 0);
    BaseTask path = BaseTask.create(ti, mClock.millis(), a -> null);
    Mockito.doAnswer(ans -> {
      path.onComplete(ans.getArgument(1), Mockito.mock(SyncResult.class));
      return null;
    }).when(mMdSync).onPathLoadComplete(anyLong(), anyBoolean(), any(SyncResult.class));
    completeFirstLoadRequestEmpty(path);

    Future<Boolean> waiter = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path")));
    assertThrows(TimeoutException.class, () -> waiter.get(1, TimeUnit.SECONDS));
    path.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path"),
            new AlluxioURI("/path")), false, true, Mockito.mock(SyncResult.class), false));
    assertTrue(waiter.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void TestMultiWaiter() throws Exception {
    TaskInfo ti = new TaskInfo(mMdSync, new AlluxioURI("/path"),
        new AlluxioURI("/path"), null,
        ALL, 0, mDirLoadType, 0);
    BaseTask path = BaseTask.create(ti, mClock.millis(), a -> null);
    Mockito.doAnswer(ans -> {
      path.onComplete(ans.getArgument(1), Mockito.mock(SyncResult.class));
      return null;
    }).when(mMdSync).onPathLoadComplete(anyLong(), anyBoolean(), any(SyncResult.class));
    completeFirstLoadRequestEmpty(path);

    Future<Boolean> waiter1 = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path/1")));
    Future<Boolean> waiter2 = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path/2")));
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    path.nextCompleted(new SyncProcessResult(ti, new AlluxioURI("/path/1"),
        new PathSequence(new AlluxioURI("/path/1"),
            new AlluxioURI("/path/1")), false, false, Mockito.mock(SyncResult.class), false));
    assertTrue(waiter1.get(1, TimeUnit.SECONDS));
    // if the path is truncated, it should not release the waiter on the path
    path.nextCompleted(new SyncProcessResult(ti, new AlluxioURI("/path/2"),
        new PathSequence(new AlluxioURI("/path/2"),
            new AlluxioURI("/path/2")), true, false, Mockito.mock(SyncResult.class), false));
    assertThrows(TimeoutException.class, () -> waiter2.get(1, TimeUnit.SECONDS));
    path.nextCompleted(new SyncProcessResult(ti, new AlluxioURI("/path/2"),
        new PathSequence(new AlluxioURI("/path/2"),
            new AlluxioURI("/path/2")), false, false, Mockito.mock(SyncResult.class), false));
    assertTrue(waiter2.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void TestNestedWaiter() throws Exception {
    TaskInfo ti = new TaskInfo(mMdSync, new AlluxioURI("/path"),
        new AlluxioURI("/path"), null,
        ALL, 0, mDirLoadType, 0);
    BaseTask path = BaseTask.create(ti, mClock.millis(), a -> null);
    Mockito.doAnswer(ans -> {
      path.onComplete(ans.getArgument(1), Mockito.mock(SyncResult.class));
      return null;
    }).when(mMdSync).onPathLoadComplete(anyLong(), anyBoolean(), any(SyncResult.class));
    completeFirstLoadRequestEmpty(path);

    Future<Boolean> waiter1 = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path/1")));
    Future<Boolean> waiter2 = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path/2")));
    // a different nested path should not release the waiters
    path.nextCompleted(new SyncProcessResult(ti, new AlluxioURI("/path/other"),
        new PathSequence(new AlluxioURI("/path/1"),
            new AlluxioURI("/path/1")), false, false, Mockito.mock(SyncResult.class), false));
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    assertThrows(TimeoutException.class, () -> waiter2.get(1, TimeUnit.SECONDS));
    // the parent path should release both the children
    path.nextCompleted(new SyncProcessResult(ti, new AlluxioURI("/path"),
        new PathSequence(new AlluxioURI("/path/1"),
            new AlluxioURI("/path/1")), false, false, Mockito.mock(SyncResult.class), false));
    assertTrue(waiter1.get(1, TimeUnit.SECONDS));
    assertTrue(waiter2.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void TestParentWaiter() throws Exception {
    long loadRequestID = 0;
    TaskInfo ti = new TaskInfo(mMdSync, new AlluxioURI("/"),
        new AlluxioURI("/path"), null,
        ALL, 0, mDirLoadType, 0);
    BaseTask path = BaseTask.create(ti, mClock.millis(), a -> null);
    Mockito.doAnswer(ans -> {
      path.onComplete(ans.getArgument(1), Mockito.mock(SyncResult.class));
      return null;
    }).when(mMdSync).onPathLoadComplete(anyLong(), anyBoolean(), any(SyncResult.class));
    completeFirstLoadRequestEmpty(path);
    loadRequestID++;

    Future<Boolean> waiter1 = mThreadPool.submit(() ->
        path.waitForSync(new AlluxioURI("/path/nested/1")));
    Future<Boolean> waiter2 = mThreadPool.submit(() ->
        path.waitForSync(new AlluxioURI("/path/nested")));
    Future<Boolean> waiter3 = mThreadPool.submit(() ->
        path.waitForSync(new AlluxioURI("/path")));
    // finishing the root should only release the direct children
    path.nextCompleted(new SyncProcessResult(ti, new AlluxioURI("/"),
        new PathSequence(new AlluxioURI("/path/1"),
            new AlluxioURI("/path/1")), false, false, Mockito.mock(SyncResult.class), false));
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    assertThrows(TimeoutException.class, () -> waiter2.get(1, TimeUnit.SECONDS));
    assertTrue(waiter3.get(1, TimeUnit.SECONDS));
    // finishing /path should release the direct children of /path
    SyncProcessResult finalResult = new SyncProcessResult(ti, new AlluxioURI("/path"),
        new PathSequence(new AlluxioURI("/path/1"),
            new AlluxioURI("/path/1")), false, false, Mockito.mock(SyncResult.class), false);
    path.nextCompleted(finalResult);
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    assertTrue(waiter2.get(1, TimeUnit.SECONDS));
    // finishing the whole task should release the remaining waiters
    path.getPathLoadTask().onProcessComplete(loadRequestID, finalResult);
    assertTrue(waiter1.get(1, TimeUnit.SECONDS));
  }
}
