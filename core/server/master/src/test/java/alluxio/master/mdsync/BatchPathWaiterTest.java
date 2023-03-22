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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BatchPathWaiterTest {

  ExecutorService mThreadPool;

  private final Clock mClock = Clock.systemUTC();

  @Before
  public void before() {
    mThreadPool = Executors.newCachedThreadPool();
  }

  @After
  public void after() {
    mThreadPool.shutdown();
  }

  private void onComplete(boolean isFile) {}

  private void onError(Throwable t) {
    assertNull(t);
  }

  @Test
  public void TestWaiter() throws Exception {
    TaskInfo ti = new TaskInfo(new AlluxioURI("/path"),
        NONE, DirectoryLoadType.NONE, 0);
    BaseTask path = BaseTask.create(ti, mClock.millis(), this::onComplete, this::onError);
    Future<Boolean> waiter = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path")));
    assertThrows(TimeoutException.class, () -> waiter.get(1, TimeUnit.SECONDS));
    // Complete the sync
    SyncProcessResult result = new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path"),
            new AlluxioURI("/path")), false, true);
    path.nextCompleted(result);
    // Even though we completed the path being waited for, we only release the waiter for
    // paths greater than the completed path
    assertThrows(TimeoutException.class, () -> waiter.get(1, TimeUnit.SECONDS));
    // now on completion of the task the waiter can be released
    path.getPathLoadTask().onProcessComplete(0 , result);
    assertTrue(path.isCompleted().isPresent());
    assertTrue(waiter.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void TestMultiWaiter() throws Exception {
    TaskInfo ti = new TaskInfo(new AlluxioURI("/path"),
        ONE, DirectoryLoadType.NONE, 0);
    BaseTask path = BaseTask.create(ti, mClock.millis(), this::onComplete, this::onError);
    Future<Boolean> waiter1 = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path/1")));
    Future<Boolean> waiter2 = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path/2")));
    // after completing /path/1 no waiters will be released
    path.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path"),
            new AlluxioURI("/path/1")), true, false));
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    assertThrows(TimeoutException.class, () -> waiter2.get(1, TimeUnit.SECONDS));
    // after completing /path/2, the waiter for /path/1 will be released
    SyncProcessResult result = new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path/1"),
            new AlluxioURI("/path/2")), false, false);
    path.nextCompleted(result);
    assertTrue(waiter1.get(1, TimeUnit.SECONDS));
    assertThrows(TimeoutException.class, () -> waiter2.get(1, TimeUnit.SECONDS));
    // now on completion of the task all waiters can be released
    path.getPathLoadTask().onProcessComplete(0 , result);
    assertTrue(path.isCompleted().isPresent());
    assertTrue(waiter2.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void TestWaiterOutOfOrder() throws Exception {
    TaskInfo ti = new TaskInfo(new AlluxioURI("/path"),
        ONE, DirectoryLoadType.NONE, 0);
    BaseTask path = BaseTask.create(ti, mClock.millis(), this::onComplete, this::onError);
    Future<Boolean> waiter1 = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path/1")));
    Future<Boolean> waiter2 = mThreadPool.submit(() -> path.waitForSync(new AlluxioURI("/path/2")));
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    path.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path/3"),
            new AlluxioURI("/path/4")), true, false));
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    assertThrows(TimeoutException.class, () -> waiter2.get(1, TimeUnit.SECONDS));
    path.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path/2"),
            new AlluxioURI("/path/3")), true, false));
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    assertThrows(TimeoutException.class, () -> waiter2.get(1, TimeUnit.SECONDS));
    path.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path"),
            new AlluxioURI("/path/1")), true, false));
    assertThrows(TimeoutException.class, () -> waiter1.get(1, TimeUnit.SECONDS));
    assertThrows(TimeoutException.class, () -> waiter2.get(1, TimeUnit.SECONDS));
    SyncProcessResult result = new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path/1"),
            new AlluxioURI("/path/2")), false, false);
    path.nextCompleted(result);
    assertTrue(waiter2.get(1, TimeUnit.SECONDS));
    path.getPathLoadTask().onProcessComplete(0 , result);
    assertTrue(path.isCompleted().isPresent());
  }

  @Test
  public void TestBaseTackSinglePath() {
    TaskInfo ti = new TaskInfo(new AlluxioURI("/path"),
        NONE, DirectoryLoadType.NONE, 0);
    BaseTask path = BaseTask.create(ti, mClock.millis(), this::onComplete, this::onError);
    assertFalse(path.isCompleted().isPresent());
    SyncProcessResult result = new SyncProcessResult(ti, ti.getBasePath(),
        new PathSequence(new AlluxioURI("/path"),
            new AlluxioURI("/path")), false, false);
    path.nextCompleted(result);
    path.getPathLoadTask().onProcessComplete(0 , result);
    assertTrue(path.isCompleted().isPresent());
  }

  @Test
  public void TestBaseTaskInOrder() {
    TaskInfo ti = new TaskInfo(new AlluxioURI("/"),
        ALL, DirectoryLoadType.NONE, 0);
    BatchPathWaiter root = (BatchPathWaiter) BaseTask.create(
        ti, mClock.millis(), this::onComplete, this::onError);
    assertFalse(root.isCompleted().isPresent());

    // complete </, /ad>, should have |<,/ad>|
    PathSequence completed = new PathSequence(new AlluxioURI("/"),
        new AlluxioURI("/ad"));
    List<PathSequence> completedList = Lists.newArrayList(
        new PathSequence(new AlluxioURI(""), new AlluxioURI("/ad")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </ad, /bf>, should have |<,/bf>|
    completed = new PathSequence(new AlluxioURI("/ad"), new AlluxioURI("/bf"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/bf")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </bf, /bf/eg>, should have |<,/bf/eg|
    completed = new PathSequence(new AlluxioURI("/bf"), new AlluxioURI("/bf/eg"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""),
        new AlluxioURI("/bf/eg")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </bf/eg, /tr>, should have |<,/tr|
    completed = new PathSequence(new AlluxioURI("/bf/eg"), new AlluxioURI("/tr"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/tr")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false));
    assertEquals(completedList, root.getLastCompleted());

    // finish with </tr, /trd>
    completed = new PathSequence(new AlluxioURI("/tr"), new AlluxioURI("/trd"));
    SyncProcessResult finalResult = new SyncProcessResult(ti, ti.getBasePath(), completed,
        false, false);
    root.nextCompleted(finalResult);
    root.getPathLoadTask().onProcessComplete(0 , finalResult);
    assertTrue(root.isCompleted().isPresent());
  }

  @Test
  public void TestBaseTaskOutOfOrder() {
    TaskInfo ti = new TaskInfo(new AlluxioURI("/"),
        ONE, DirectoryLoadType.NONE, 0);
    BatchPathWaiter root = (BatchPathWaiter) BaseTask.create(ti, mClock.millis(),
        this::onComplete, this::onError);
    assertFalse(root.isCompleted().isPresent());

    // complete </, /a>, should have |<,a>|
    PathSequence completed = new PathSequence(new AlluxioURI("/"), new AlluxioURI("/a"));
    List<PathSequence> completedList = Lists.newArrayList(
        new PathSequence(new AlluxioURI(""), new AlluxioURI("/a")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </a, /b>, should have |<,b>|
    completed = new PathSequence(new AlluxioURI("/a"), new AlluxioURI("/b"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/b")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </c, /d>, should have |<, /b>, </c, /d>|
    completed = new PathSequence(new AlluxioURI("/c"), new AlluxioURI("/d"));
    completedList.add(completed);
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </b, /c>, should have |<,/d>|
    completed = new PathSequence(new AlluxioURI("/b"), new AlluxioURI("/c"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/d")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </g, /h>, should have |<,/d>, </g, /h>|
    completed = new PathSequence(new AlluxioURI("/g"), new AlluxioURI("/h"));
    completedList.add(completed);
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </d,/e>, should have |<,/e>, </g, /h>|
    completed = new PathSequence(new AlluxioURI("/d"), new AlluxioURI("/e"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/e")),
        new PathSequence(new AlluxioURI("/g"), new AlluxioURI("/h")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </f,/g>, should have |<,/e>, </f, /h>|
    completed = new PathSequence(new AlluxioURI("/f"), new AlluxioURI("/g"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/e")),
        new PathSequence(new AlluxioURI("/f"), new AlluxioURI("/h")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false));
    assertEquals(completedList, root.getLastCompleted());

    // complete </e,/f>, should have |<,/h>|
    completed = new PathSequence(new AlluxioURI("/e"), new AlluxioURI("/f"));
    completedList = Lists.newArrayList(new PathSequence(new AlluxioURI(""), new AlluxioURI("/h")));
    root.nextCompleted(new SyncProcessResult(ti, ti.getBasePath(), completed, true, false));
    assertEquals(completedList, root.getLastCompleted());

    // finish with </h, /j>
    completed = new PathSequence(new AlluxioURI("/h"), new AlluxioURI("/j"));
    SyncProcessResult finalResult = new SyncProcessResult(ti, ti.getBasePath(), completed,
        false, false);
    root.nextCompleted(finalResult);
    root.getPathLoadTask().onProcessComplete(0, finalResult);
    assertTrue(root.isCompleted().isPresent());
  }
}
