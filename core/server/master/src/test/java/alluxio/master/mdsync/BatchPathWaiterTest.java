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
import static alluxio.file.options.DescendantType.NONE;
import static alluxio.file.options.DescendantType.ONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;

import alluxio.AlluxioURI;
import alluxio.file.options.DirectoryLoadType;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsClient;
import alluxio.underfs.UfsLoadResult;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Stream;

public class BatchPathWaiterTest {

  ExecutorService mThreadPool;

  private final Clock mClock = Clock.systemUTC();
  private MdSync mMdSync;

  private final MockUfsClient mUfsClient = new MockUfsClient();

  private final Function<AlluxioURI, CloseableResource<UfsClient>> mClientSupplier =
      (uri) -> new CloseableResource<UfsClient>(mUfsClient) {
        @Override
        public void closeResource() {}
      };

  @Before
  public void before() {
    mThreadPool = Executors.newCachedThreadPool();
    mMdSync = Mockito.spy(new MdSync(Mockito.mock(TaskTracker.class)));
  }

  @After
  public void after() {
    mThreadPool.shutdown();
  }

  static void completeFirstLoadRequestEmpty(BaseTask task) {
    // the initial load of metadata sync is a getStatus request
    // here we complete it as an empty value, which
    // can happen for example when loading a directory from an ObjectStore
    task.getPathLoadTask().createLoadResult(0,
        new UfsLoadResult(Stream.empty(), 0, null, null, false, false, true));
  }

  @Test
  public void TestWaiter() throws Exception {
    long nxtLoadID = 0;
    TaskInfo ti = new TaskInfo(mMdSync, new AlluxioURI("/path"),
        new AlluxioURI("/path"), null,
        NONE, 0, DirectoryLoadType.SINGLE_LISTING, 0);
    BaseTask path = BaseTask.create(ti, mClock.millis(), mClientSupplier);
    Mockito.doAnswer(ans -> {
      path.onComplete(ans.getArgument(1));
      return null;
    }).when(mMdSync).onPathLoadComplete(anyLong(), anyBoolean());
    completeFirstLoadRequestEmpty(path);
    nxtLoadID++;

    Future<Boolean> waiter = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path")));
    assertThrows(TimeoutException.class, () -> waiter.get(1, TimeUnit.SECONDS));
    // Complete the sync
    path.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), null,
        false, false, true));
    SyncProcessResult result = new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path"),
            new AlluxioURI("/path")), false, true, false);
    path.nextCompleted(result);
    // Even though we completed the path being waited for, we only release the waiter for
    // paths greater than the completed path
    assertThrows(TimeoutException.class, () -> waiter.get(1, TimeUnit.SECONDS));
    // now on completion of the task the waiter can be released
    path.getPathLoadTask().onProcessComplete(nxtLoadID, result);
    assertTrue(path.isCompleted().isPresent());
    assertTrue(waiter.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void TestMultiWaiter() throws Exception {
    long nxtLoadID = 0;
    TaskInfo ti = new TaskInfo(mMdSync, new AlluxioURI("/path"),
        new AlluxioURI("/path"), null,
        ONE, 0, DirectoryLoadType.SINGLE_LISTING, 0);
    BaseTask path = BaseTask.create(ti, mClock.millis(), mClientSupplier);
    Mockito.doAnswer(ans -> {
      path.onComplete(ans.getArgument(1));
      return null;
    }).when(mMdSync).onPathLoadComplete(anyLong(), anyBoolean());
    completeFirstLoadRequestEmpty(path);
    nxtLoadID++;

    Future<Boolean> waiter1 = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path/1")));
    Future<Boolean> waiter2 = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path/2")));
    // after completing /path/1 no waiters will be released
    path.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path"),
            new AlluxioURI("/path/1")), true, false, false));
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    assertThrows(TimeoutException.class, () -> waiter2.get(1, TimeUnit.SECONDS));
    // after completing /path/2, the waiter for /path/1 will be released
    SyncProcessResult result = new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path/1"),
            new AlluxioURI("/path/2")), false, false, false);
    path.nextCompleted(result);
    assertTrue(waiter1.get(1, TimeUnit.SECONDS));
    assertThrows(TimeoutException.class, () -> waiter2.get(1, TimeUnit.SECONDS));
    // now on completion of the task all waiters can be released
    path.getPathLoadTask().onProcessComplete(nxtLoadID, result);
    assertTrue(path.isCompleted().isPresent());
    assertTrue(waiter2.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void TestWaiterOutOfOrder() throws Exception {
    long nxtLoadID = 0;
    TaskInfo ti = new TaskInfo(mMdSync, new AlluxioURI("/path"),
        new AlluxioURI("/path"), null,
        ONE, 0, DirectoryLoadType.SINGLE_LISTING, 0);
    BaseTask path = BaseTask.create(ti, mClock.millis(), mClientSupplier);
    Mockito.doAnswer(ans -> {
      path.onComplete(ans.getArgument(1));
      return null;
    }).when(mMdSync).onPathLoadComplete(anyLong(), anyBoolean());
    completeFirstLoadRequestEmpty(path);
    nxtLoadID++;

    Future<Boolean> waiter1 = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path/1")));
    Future<Boolean> waiter2 = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path/2")));
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    path.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path/3"),
            new AlluxioURI("/path/4")), true, false, false));
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    assertThrows(TimeoutException.class, () -> waiter2.get(1, TimeUnit.SECONDS));
    path.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path/2"),
            new AlluxioURI("/path/3")), true, false, false));
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    assertThrows(TimeoutException.class, () -> waiter2.get(1, TimeUnit.SECONDS));
    path.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path"),
            new AlluxioURI("/path/1")), true, false, false));
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    assertThrows(TimeoutException.class, () -> waiter2.get(1, TimeUnit.SECONDS));
    SyncProcessResult result = new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path/1"),
            new AlluxioURI("/path/2")), false, false, false);
    path.nextCompleted(result);
    assertTrue(waiter2.get(1, TimeUnit.SECONDS));
    path.getPathLoadTask().onProcessComplete(nxtLoadID, result);
    assertTrue(path.isCompleted().isPresent());
  }

  @Test
  public void TestBaseTackSinglePath() {
    long nxtLoadID = 0;
    TaskInfo ti = new TaskInfo(mMdSync, new AlluxioURI("/path"),
        new AlluxioURI("/path"), null,
        NONE, 0, DirectoryLoadType.SINGLE_LISTING, 0);
    BaseTask path = BaseTask.create(ti, mClock.millis(), mClientSupplier);
    Mockito.doAnswer(ans -> {
      path.onComplete(ans.getArgument(1));
      return null;
    }).when(mMdSync).onPathLoadComplete(anyLong(), anyBoolean());
    completeFirstLoadRequestEmpty(path);
    nxtLoadID++;

    assertFalse(path.isCompleted().isPresent());
    SyncProcessResult result = new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path"),
            new AlluxioURI("/path")), false, false, false);
    path.nextCompleted(result);
    path.getPathLoadTask().onProcessComplete(nxtLoadID, result);
    assertTrue(path.isCompleted().isPresent());
  }

  @Test
  public void TestBaseTaskInOrder() {
    long nxtLoadID = 0;
    TaskInfo ti = new TaskInfo(mMdSync, new AlluxioURI("/"),
        new AlluxioURI("/"), null,
        ALL, 0, DirectoryLoadType.SINGLE_LISTING, 0);
    BatchPathWaiter root = (BatchPathWaiter) BaseTask.create(
        ti, mClock.millis(), mClientSupplier);
    Mockito.doAnswer(ans -> {
      root.onComplete(ans.getArgument(1));
      return null;
    }).when(mMdSync).onPathLoadComplete(anyLong(), anyBoolean());
    assertFalse(root.isCompleted().isPresent());
    completeFirstLoadRequestEmpty(root);
    nxtLoadID++;

    // complete </, /ad>, should have |<,/ad>|
    PathSequence completed = new PathSequence(new AlluxioURI("/"),
        new AlluxioURI("/ad"));
    List<PathSequence> completedList = Lists.newArrayList(
        new PathSequence(new AlluxioURI(""), new AlluxioURI("/ad")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true,
        false, false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </ad, /bf>, should have |<,/bf>|
    completed = new PathSequence(new AlluxioURI("/ad"), new AlluxioURI("/bf"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/bf")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true,
        false, false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </bf, /bf/eg>, should have |<,/bf/eg|
    completed = new PathSequence(new AlluxioURI("/bf"), new AlluxioURI("/bf/eg"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""),
        new AlluxioURI("/bf/eg")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true,
        false, false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </bf/eg, /tr>, should have |<,/tr|
    completed = new PathSequence(new AlluxioURI("/bf/eg"), new AlluxioURI("/tr"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/tr")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true,
        false, false));
    assertEquals(completedList, root.getLastCompleted());

    // finish with </tr, /trd>
    completed = new PathSequence(new AlluxioURI("/tr"), new AlluxioURI("/trd"));
    SyncProcessResult finalResult = new SyncProcessResult(ti, ti.getBasePath(), completed,
        false, false, false);
    root.nextCompleted(finalResult);
    root.getPathLoadTask().onProcessComplete(nxtLoadID, finalResult);
    assertTrue(root.isCompleted().isPresent());
  }

  @Test
  public void TestBaseTaskOutOfOrder() {
    long nxtLoadID = 0;
    TaskInfo ti = new TaskInfo(mMdSync, new AlluxioURI("/"),
        new AlluxioURI("/"), null,
        ONE, 0, DirectoryLoadType.SINGLE_LISTING, 0);
    BatchPathWaiter root = (BatchPathWaiter) BaseTask.create(ti, mClock.millis(), mClientSupplier);
    Mockito.doAnswer(ans -> {
      root.onComplete(ans.getArgument(1));
      return null;
    }).when(mMdSync).onPathLoadComplete(anyLong(), anyBoolean());
    assertFalse(root.isCompleted().isPresent());
    completeFirstLoadRequestEmpty(root);
    nxtLoadID++;

    // complete </, /a>, should have |<,a>|
    PathSequence completed = new PathSequence(new AlluxioURI("/"), new AlluxioURI("/a"));
    List<PathSequence> completedList = Lists.newArrayList(
        new PathSequence(new AlluxioURI(""), new AlluxioURI("/a")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false,
        false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </a, /b>, should have |<,b>|
    completed = new PathSequence(new AlluxioURI("/a"), new AlluxioURI("/b"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/b")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false,
        false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </c, /d>, should have |<, /b>, </c, /d>|
    completed = new PathSequence(new AlluxioURI("/c"), new AlluxioURI("/d"));
    completedList.add(completed);
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false,
        false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </b, /c>, should have |<,/d>|
    completed = new PathSequence(new AlluxioURI("/b"), new AlluxioURI("/c"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/d")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false,
        false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </g, /h>, should have |<,/d>, </g, /h>|
    completed = new PathSequence(new AlluxioURI("/g"), new AlluxioURI("/h"));
    completedList.add(completed);
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false,
        false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </d,/e>, should have |<,/e>, </g, /h>|
    completed = new PathSequence(new AlluxioURI("/d"), new AlluxioURI("/e"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/e")),
        new PathSequence(new AlluxioURI("/g"), new AlluxioURI("/h")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false,
        false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </f,/g>, should have |<,/e>, </f, /h>|
    completed = new PathSequence(new AlluxioURI("/f"), new AlluxioURI("/g"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/e")),
        new PathSequence(new AlluxioURI("/f"), new AlluxioURI("/h")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false,
        false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </e,/f>, should have |<,/h>|
    completed = new PathSequence(new AlluxioURI("/e"), new AlluxioURI("/f"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/h")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false,
        false));
    assertEquals(completedList, root.getLastCompleted());

    // finish with </h, /j>
    completed = new PathSequence(new AlluxioURI("/h"), new AlluxioURI("/j"));
    SyncProcessResult finalResult = new SyncProcessResult(ti, ti.getBasePath(), completed,
        false, false, false);
    root.nextCompleted(finalResult);
    root.getPathLoadTask().onProcessComplete(nxtLoadID, finalResult);
    assertTrue(root.isCompleted().isPresent());
  }
}
