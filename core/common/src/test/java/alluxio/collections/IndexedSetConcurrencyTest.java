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

package alluxio.collections;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import alluxio.util.SleepUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test concurrent behavior of {@link IndexedSet}.
 */
public class IndexedSetConcurrencyTest {
  /** The maximum value for the size value for the test object. */
  private static final int MAX_SIZE = 30;
  /** The duration for each test. */
  private static final int TEST_CASE_DURATION_MS = 5000;
  /** The minimum number of threads for each task type. */
  private static final int MIN_TASKS = 3;
  /** The maximum number of threads for each task type. */
  private static final int MAX_TASKS = 6;
  /** the maximum repeatable times in one task of size in {@link TestInfo}. */
  private static final int MAX_REPEAT_TIMES = 6;

  private IndexedSet<TestInfo> mIndexedSet;
  private ExecutorService mThreadPool;
  /** Used to stop concurrent threads. */
  private AtomicBoolean mStopThreads;

  /**
   * Base class for testing different behaviours of {@link IndexedSet} concurrently.
   */
  private abstract class ConcurrentTask implements Callable<Void> {
    /** Number of items added or deleted in this single task. */
    private volatile long mCount = 0;
    private CyclicBarrier mBarrier;

    /**
     * @param barrier the CyclicBarrier
     */
    public ConcurrentTask(CyclicBarrier barrier) {
      mBarrier = barrier;
    }

    public long getCount() {
      return mCount;
    }

    /**
     * Runs a single task.
     *
     * @return number of items added or deleted
     */
    abstract long runSingleTask();

    @Override
    public Void call() throws BrokenBarrierException, InterruptedException {
      mBarrier.await();
      while (!mStopThreads.get()) {
        mCount += runSingleTask();
      }
      return null;
    }
  }

  /**
   * A class for testing add behaviour of {@link IndexedSet} concurrently.
   */
  private class ConcurrentAdd extends ConcurrentTask {
    public ConcurrentAdd(CyclicBarrier barrier) {
      super(barrier);
    }

    @Override
    public long runSingleTask() {
      return mIndexedSet.add(new TestInfo()) ? 1 : 0;
    }
  }

  /**
   * A class for testing add behaviour of {@link IndexedSet} concurrently and checking if
   * the result is valid.
   */
  private class ConcurrentAddWithCheck extends ConcurrentTask {
    public ConcurrentAddWithCheck(CyclicBarrier barrier) {
      super(barrier);
    }

    @Override
    public long runSingleTask() {
      long result = 0;
      int size = ThreadLocalRandom.current().nextInt(0, MAX_SIZE);

      for (int i = ThreadLocalRandom.current().nextInt(1, MAX_REPEAT_TIMES + 1); i > 0; i--) {
        TestInfo instance = new TestInfo(ThreadLocalRandom.current().nextLong(), size);
        result += (mIndexedSet.add(instance) ? 1 : 0);
        assertTrue(mIndexedSet.contains(mIdIndex, instance.getId()));
        assertEquals(1, mIndexedSet.getByField(mIdIndex, instance.getId()).size());
      }

      assertTrue(result <= mIndexedSet.getByField(mSizeIndex, size).size());

      return result;
    }
  }

  /**
   * A class for testing remove behaviour of {@link IndexedSet} concurrently.
   */
  private class ConcurrentRemove extends ConcurrentTask {
    public ConcurrentRemove(CyclicBarrier barrier) {
      super(barrier);
    }

    @Override
    public long runSingleTask() {
      TestInfo info =
          mIndexedSet.getFirstByField(mSizeIndex, ThreadLocalRandom.current().nextInt(0, MAX_SIZE));
      if (info != null) {
        return mIndexedSet.remove(info) ? 1 : 0;
      }
      return 0;
    }
  }

  /**
   * Removes concurrent tasks by field.
   */
  private class ConcurrentRemoveByField extends ConcurrentTask {
    public ConcurrentRemoveByField(CyclicBarrier barrier) {
      super(barrier);
    }

    @Override
    public long runSingleTask() {
      return mIndexedSet
          .removeByField(mSizeIndex, ThreadLocalRandom.current().nextInt(0, MAX_SIZE));
    }
  }

  /**
   * Removes concurrent tasks by iterator.
   */
  private class ConcurrentRemoveByIterator extends ConcurrentTask {
    public ConcurrentRemoveByIterator(CyclicBarrier barrier) {
      super(barrier);
    }

    @Override
    public long runSingleTask() {
      long removed = 0;
      Iterator<TestInfo> it = mIndexedSet.iterator();
      while (it.hasNext()) {
        it.next();
        it.remove();
        removed++;
      }
      return removed;
    }
  }

  /**
   * Clears out concurrent tasks.
   */
  private class ConcurrentClear extends ConcurrentTask {
    public ConcurrentClear(CyclicBarrier barrier) {
      super(barrier);
    }

    @Override
    public long runSingleTask() {
      mIndexedSet.clear();
      return 1;
    }
  }

  /**
   * Helper class for testing {@link IndexedSet}.
   */
  private final class TestInfo {
    private final long mId;
    private final int mSize;

    /**
     * Creates an instance of {@link TestInfo} randomly.
     */
    private TestInfo() {
      this(ThreadLocalRandom.current().nextLong(),
          ThreadLocalRandom.current().nextInt(0, MAX_SIZE));
    }

    /**
     * Creates an instance of {@link TestInfo} by giving its id and size fields.
     *
     * @param id the id
     * @param size the size
     */
    private TestInfo(long id, int size) {
      mId = id;
      mSize = size;
    }

    public long getId() {
      return mId;
    }

    public int getSize() {
      return mSize;
    }
  }

  private final IndexDefinition<TestInfo, Long> mIdIndex =
      new IndexDefinition<TestInfo, Long>(true) {
        @Override
        public Long getFieldValue(TestInfo o) {
          return o.getId();
        }
      };

  private final IndexDefinition<TestInfo, Integer> mSizeIndex =
      new IndexDefinition<TestInfo, Integer>(false) {
        @Override
        public Integer getFieldValue(TestInfo o) {
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
    assertEquals(expectedCount, mIndexedSet.size());

    // Verify the size according to the id index.
    int count = 0;
    for (Long id : ids) {
      Set<TestInfo> elements = mIndexedSet.getByField(mIdIndex, id);
      count += elements.size();
    }
    assertEquals(expectedCount, count);

    // Verify the size according to the size index.
    count = 0;
    for (Integer size : sizes) {
      Set<TestInfo> elements = mIndexedSet.getByField(mSizeIndex, size);
      count += elements.size();
    }
    assertEquals(expectedCount, count);
  }

  @Test
  public void basicConcurrentUpdate() throws Exception {
    List<Future<?>> futures = new ArrayList<>();
    List<ConcurrentTask> addTasks = new ArrayList<>();
    List<ConcurrentTask> removeTasks = new ArrayList<>();
    int[] tasksNumbers = new int[3];
    int totalTasksNumber = 0;

    // Try to balance adds and removes
    tasksNumbers[0] = 2 * ThreadLocalRandom.current().nextInt(MIN_TASKS, MAX_TASKS + 1);
    totalTasksNumber += tasksNumbers[0];
    // Add random number of each task type.
    for (int i = 1; i < 3; i++) {
      tasksNumbers[i] = ThreadLocalRandom.current().nextInt(MIN_TASKS, MAX_TASKS + 1);
      totalTasksNumber += tasksNumbers[i];
    }

    CyclicBarrier barrier = new CyclicBarrier(totalTasksNumber);

    // Add random number of each task type.
    for (int i = 0; i < tasksNumbers[0]; i++) {
      // Try to balance adds and removes
      addTasks.add(new ConcurrentAdd(barrier));
    }

    for (int i = 0; i < tasksNumbers[1]; i++) {
      removeTasks.add(new ConcurrentRemove(barrier));
    }

    for (int i = 0; i < tasksNumbers[2]; i++) {
      removeTasks.add(new ConcurrentRemoveByField(barrier));
    }

    for (ConcurrentTask task : addTasks) {
      futures.add(mThreadPool.submit(task));
    }
    for (ConcurrentTask task : removeTasks) {
      futures.add(mThreadPool.submit(task));
    }

    SleepUtils.sleepMs(TEST_CASE_DURATION_MS);
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

    assertEquals(mIndexedSet.size(), added - removed);
    verifySet();
  }

  @Test
  public void concurrentUpdate() throws Exception {
    List<Future<?>> futures = new ArrayList<>();
    int[] tasksNumbers = new int[5];
    int totalTasksNumber = 0;

    // Try to balance adds and removes
    tasksNumbers[0] = 4 * ThreadLocalRandom.current().nextInt(MIN_TASKS, MAX_TASKS + 1);
    totalTasksNumber += tasksNumbers[0];
    // Add random number of each task type.
    for (int i = 1; i < 5; i++) {
      tasksNumbers[i] = ThreadLocalRandom.current().nextInt(MIN_TASKS, MAX_TASKS + 1);
      totalTasksNumber += tasksNumbers[i];
    }

    CyclicBarrier barrier = new CyclicBarrier(totalTasksNumber);

    for (int i = 0; i < tasksNumbers[0]; i++) {
      futures.add(mThreadPool.submit(new ConcurrentAdd(barrier)));
    }
    for (int i = 0; i < tasksNumbers[1]; i++) {
      futures.add(mThreadPool.submit(new ConcurrentRemove(barrier)));
    }
    for (int i = 0; i < tasksNumbers[2]; i++) {
      futures.add(mThreadPool.submit(new ConcurrentRemoveByField(barrier)));
    }
    for (int i = 0; i < tasksNumbers[3]; i++) {
      futures.add(mThreadPool.submit(new ConcurrentRemoveByIterator(barrier)));
    }
    for (int i = 0; i < tasksNumbers[4]; i++) {
      futures.add(mThreadPool.submit(new ConcurrentClear(barrier)));
    }

    SleepUtils.sleepMs(TEST_CASE_DURATION_MS);
    mStopThreads.set(true);
    for (Future<?> future : futures) {
      future.get();
    }
    verifySet();
  }

  @Test
  public void concurrentAdd() throws Exception {
    List<Future<?>> futures = new ArrayList<>();

    // Add random number of each task type.
    int tasksNumber = 2 * ThreadLocalRandom.current().nextInt(MIN_TASKS, MAX_TASKS + 1);
    CyclicBarrier barrier = new CyclicBarrier(tasksNumber);

    for (int i = 0; i < tasksNumber; i++) {
      futures.add(mThreadPool.submit(new ConcurrentAddWithCheck(barrier)));
    }

    SleepUtils.sleepMs(TEST_CASE_DURATION_MS);
    mStopThreads.set(true);
    for (Future<?> future : futures) {
      future.get();
    }

    verifySet();
  }

  /**
   * Use the mSizeIndex as primary index, test the correctness of using non-unique index as primary
   * index.
   */
  @Test
  public void nonUniqueConcurrentUpdate() throws Exception {
    mIndexedSet = new IndexedSet<>(mSizeIndex, mIdIndex);
    List<Future<?>> futures = new ArrayList<>();
    int[] tasksNumbers = new int[5];
    int totalTasksNumber = 0;

    // Try to balance adds and removes
    tasksNumbers[0] = 4 * ThreadLocalRandom.current().nextInt(MIN_TASKS, MAX_TASKS + 1);
    totalTasksNumber += tasksNumbers[0];
    // Add random number of each task type.
    for (int i = 1; i < 5; i++) {
      tasksNumbers[i] = ThreadLocalRandom.current().nextInt(MIN_TASKS, MAX_TASKS + 1);
      totalTasksNumber += tasksNumbers[i];
    }

    CyclicBarrier barrier = new CyclicBarrier(totalTasksNumber);

    for (int i = 0; i < tasksNumbers[0]; i++) {
      futures.add(mThreadPool.submit(new ConcurrentAdd(barrier)));
    }
    for (int i = 0; i < tasksNumbers[1]; i++) {
      futures.add(mThreadPool.submit(new ConcurrentRemove(barrier)));
    }
    for (int i = 0; i < tasksNumbers[2]; i++) {
      futures.add(mThreadPool.submit(new ConcurrentRemoveByField(barrier)));
    }
    for (int i = 0; i < tasksNumbers[3]; i++) {
      futures.add(mThreadPool.submit(new ConcurrentRemoveByIterator(barrier)));
    }
    for (int i = 0; i < tasksNumbers[4]; i++) {
      futures.add(mThreadPool.submit(new ConcurrentClear(barrier)));
    }

    SleepUtils.sleepMs(TEST_CASE_DURATION_MS);
    mStopThreads.set(true);
    for (Future<?> future : futures) {
      future.get();
    }
    verifySet();
  }
}
