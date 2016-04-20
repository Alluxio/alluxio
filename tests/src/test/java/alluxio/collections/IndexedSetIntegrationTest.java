/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.collections;

import alluxio.util.CommonUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test concurrent behavior of {@link IndexedSet}.
 */
public class IndexedSetIntegrationTest {
  /** The maximum value for the size value for the test object. */
  private static final int MAX_SIZE = 30;
  /** The duration for each test. */
  private static final int TEST_CASE_DURATION_MS = 5000;

  private IndexedSet<TestInfo> mIndexedSet;
  private ExecutorService mThreadPool;
  /** Used to stop concurrent threads. */
  private AtomicBoolean mStopThreads;

  private abstract class ConcurrentTask implements Runnable {
    protected long mCount = 0;

    public long getCount() {
      return mCount;
    }
  }

  private class ConcurrentAdd extends ConcurrentTask {
    @Override
    public void run() {
      while (!mStopThreads.get()) {
        if (mIndexedSet.add(new TestInfo())) {
          mCount++;
        }
      }
    }
  }

  private class ConcurrentRemove extends ConcurrentTask {
    @Override
    public void run() {
      while (!mStopThreads.get()) {
        TestInfo info = mIndexedSet.getFirstByField(mSizeIndex,
            java.util.concurrent.ThreadLocalRandom.current().nextInt(0, MAX_SIZE));
        if (info != null) {
          if (mIndexedSet.remove(info)) {
            mCount++;
          }
        }
      }
    }
  }

  private class ConcurrentRemoveByField extends ConcurrentTask {
    @Override
    public void run() {
      while (!mStopThreads.get()) {
        mCount += mIndexedSet.removeByField(mSizeIndex,
            java.util.concurrent.ThreadLocalRandom.current().nextInt(0, MAX_SIZE));
      }
    }
  }

  private class ConcurrentRemoveByIterator extends ConcurrentTask {
    @Override
    public void run() {
      while (!mStopThreads.get()) {
        Iterator<TestInfo> it = mIndexedSet.iterator();
        while (it.hasNext()) {
          it.next();
          it.remove();
          mCount++;
        }
      }
    }
  }

  private class ConcurrentClear extends ConcurrentTask {
    @Override
    public void run() {
      while (!mStopThreads.get()) {
        mIndexedSet.clear();
        mCount++;
      }
    }
  }

  private final class TestInfo {
    private long mId;
    private int mSize;

    private TestInfo() {
      mId = java.util.concurrent.ThreadLocalRandom.current().nextLong();
      mSize = java.util.concurrent.ThreadLocalRandom.current().nextInt(0, MAX_SIZE);
    }

    public long getId() {
      return mId;
    }

    public int getSize() {
      return mSize;
    }
  }

  private final IndexedSet.FieldIndex<TestInfo> mIdIndex =
      new IndexedSet.FieldIndex<TestInfo>() {
        @Override
        public Object getFieldValue(TestInfo o) {
          return o.getId();
        }
      };

  private final IndexedSet.FieldIndex<TestInfo> mSizeIndex =
      new IndexedSet.FieldIndex<TestInfo>() {
        @Override
        public Object getFieldValue(TestInfo o) {
          return o.getSize();
        }
      };

  @Before
  public void before() throws Exception {
    mIndexedSet = new IndexedSet<>(mIdIndex, mSizeIndex);
    mThreadPool = Executors.newCachedThreadPool();
    mStopThreads = new AtomicBoolean(false);
  }

  @After
  public void after() {
    mThreadPool.shutdownNow();
  }

  /**
   * Verifies the {@link #mIndexedSet} for internal consistency.
   */
  private void verifySet() {
    Iterator<TestInfo> it = mIndexedSet.iterator();
    Set<Long> ids = new HashSet<>();
    Set<Integer> sizes = new HashSet<>();

    // Verify the size.
    int expectedCount = 0;
    while (it.hasNext()) {
      TestInfo info = it.next();
      ids.add(info.getId());
      sizes.add(info.getSize());
      expectedCount++;
    }
    Assert.assertEquals(expectedCount, mIndexedSet.size());

    // Verify the size according to the id index.
    int count = 0;
    for (Long id : ids) {
      Set<TestInfo> elements = mIndexedSet.getByField(mIdIndex, id);
      count += elements.size();
    }
    Assert.assertEquals(expectedCount, count);

    // Verify the size according to the size index.
    count = 0;
    for (Integer size : sizes) {
      Set<TestInfo> elements = mIndexedSet.getByField(mSizeIndex, size);
      count += elements.size();
    }
    Assert.assertEquals(expectedCount, count);
  }

  @Test
  public void basicConcurrentTest() throws Exception {
    List<Future<?>> futures = new ArrayList<>();
    List<ConcurrentTask> addTasks = new ArrayList<>();
    List<ConcurrentTask> removeTasks = new ArrayList<>();

    addTasks.add(new ConcurrentAdd());
    addTasks.add(new ConcurrentAdd());
    addTasks.add(new ConcurrentAdd());
    addTasks.add(new ConcurrentAdd());
    removeTasks.add(new ConcurrentRemove());
    removeTasks.add(new ConcurrentRemove());
    removeTasks.add(new ConcurrentRemoveByField());
    removeTasks.add(new ConcurrentRemoveByField());
    for (ConcurrentTask task : addTasks) {
      futures.add(mThreadPool.submit(task));
    }
    for (ConcurrentTask task : removeTasks) {
      futures.add(mThreadPool.submit(task));
    }

    CommonUtils.sleepMs(TEST_CASE_DURATION_MS);
    mStopThreads.set(true);
    for (Future<?> future : futures) {
      future.get();
    }

    // Calculate how many elements have been added or removed.
    long added = 0;
    for (ConcurrentTask task : addTasks) {
      added += task.getCount();
    }
    long removed = 0;
    for (ConcurrentTask task : removeTasks) {
      removed += task.getCount();
    }

    Assert.assertEquals(mIndexedSet.size(), added - removed);
    verifySet();
  }

  @Test
  public void concurrentTest() throws Exception {
    List<Future<?>> futures = new ArrayList<>();
    List<ConcurrentTask> addTasks = new ArrayList<>();
    List<ConcurrentTask> removeTasks = new ArrayList<>();

    addTasks.add(new ConcurrentAdd());
    addTasks.add(new ConcurrentAdd());
    addTasks.add(new ConcurrentAdd());
    addTasks.add(new ConcurrentAdd());
    addTasks.add(new ConcurrentAdd());
    addTasks.add(new ConcurrentAdd());
    removeTasks.add(new ConcurrentRemove());
    removeTasks.add(new ConcurrentRemove());
    removeTasks.add(new ConcurrentRemoveByField());
    removeTasks.add(new ConcurrentRemoveByField());
    removeTasks.add(new ConcurrentRemoveByIterator());
    removeTasks.add(new ConcurrentRemoveByIterator());
    for (ConcurrentTask task : addTasks) {
      futures.add(mThreadPool.submit(task));
    }
    for (ConcurrentTask task : removeTasks) {
      futures.add(mThreadPool.submit(task));
    }

    CommonUtils.sleepMs(TEST_CASE_DURATION_MS);
    mStopThreads.set(true);
    for (Future<?> future : futures) {
      future.get();
    }
    verifySet();
  }
}
