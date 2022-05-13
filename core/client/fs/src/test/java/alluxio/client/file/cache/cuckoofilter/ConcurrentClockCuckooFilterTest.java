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

package alluxio.client.file.cache.cuckoofilter;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.client.quota.CacheScope;
import alluxio.test.util.ConcurrencyUtils;

import com.google.common.hash.Funnels;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ConcurrentClockCuckooFilterTest {
  private static final int EXPECTED_INSERTIONS = Constants.KB;
  private static final int BITS_PER_CLOCK = 4;
  private static final int MAX_AGE = (1 << BITS_PER_CLOCK) - 1;
  private static final int BITS_PER_SIZE = 20;
  private static final int BITS_PER_SCOPE = 8;

  private static final CacheScope SCOPE1 = CacheScope.create("schema1.table1");
  private static final CacheScope SCOPE2 = CacheScope.create("schema1.table2");

  // concurrency configurations
  private static final int DEFAULT_THREAD_AMOUNT = 12;
  private static final int DEFAULT_TIMEOUT_SECONDS = 100;

  private ConcurrentClockCuckooFilter<Integer> mClockFilter;

  @Before
  public void beforeTest() {
    init();
  }

  private void init() {
    mClockFilter = ConcurrentClockCuckooFilter.create(Funnels.integerFunnel(), EXPECTED_INSERTIONS,
        BITS_PER_CLOCK, BITS_PER_SIZE, BITS_PER_SCOPE);
  }

  @Test
  public void testClockField() {
    for (int i = 1; i <= MAX_AGE; i++) {
      assertTrue(mClockFilter.put(i, 1, SCOPE1));
      assertEquals(MAX_AGE, mClockFilter.getAge(i));
      mClockFilter.aging();
      for (int j = 1; j <= i; j++) {
        assertTrue(mClockFilter.mightContain(j));
        assertEquals(MAX_AGE - i + j - 1, mClockFilter.getAge(j));
      }
    }
    // fill the filter, and expect reallocation to disturb original position
    for (int i = MAX_AGE + 1; i <= EXPECTED_INSERTIONS; i++) {
      if (!mClockFilter.mightContain(i)) {
        mClockFilter.put(i, 1, SCOPE1);
      }
    }
    // re-check the age of item 1~15
    for (int i = 1; i <= MAX_AGE; i++) {
      assertTrue(mClockFilter.mightContain(i));
      assertEquals(i - 1, mClockFilter.getAge(i));
    }
  }

  @Test
  public void testSizeField() {
    int scope1Size = 0;
    for (int i = 1; i <= BITS_PER_SIZE; i++) {
      int s = (1 << i) - 1;
      assertTrue(mClockFilter.put(i, s, SCOPE1));
      scope1Size += s;
      assertEquals(scope1Size, mClockFilter.approximateElementSize(SCOPE1));
    }
    // fill the filter, and expect reallocation to disturb original position
    for (int i = MAX_AGE + 1; i <= EXPECTED_INSERTIONS; i++) {
      if (!mClockFilter.mightContain(i)) {
        mClockFilter.put(i, 1, SCOPE2);
      }
    }
    // delete some items and re-check the size of item 1~15
    assertEquals(scope1Size, mClockFilter.approximateElementSize(SCOPE1));
    for (int i = 1; i <= BITS_PER_SIZE; i++) {
      assertTrue(mClockFilter.mightContain(i));
      assertTrue(mClockFilter.delete(i));
      assertFalse(mClockFilter.mightContain(i));
      scope1Size -= ((1 << i) - 1);
      assertEquals(scope1Size, mClockFilter.approximateElementSize(SCOPE1));
    }
  }

  @Test
  public void testSizeRange() {
    // inserted item size should be in range (0, 2^BITS_PER_SIZE]
    int maxSize = (1 << BITS_PER_SIZE);
    // should fail to insert an item with a non-positive size
    assertFalse(mClockFilter.put(1, -1, SCOPE1));
    assertFalse(mClockFilter.put(1, 0, SCOPE1));

    // should handle an item with a maximum size correctly
    mClockFilter.put(1, maxSize, SCOPE1);
    mClockFilter.put(2, maxSize + 1, SCOPE1);
    assertEquals(2, mClockFilter.approximateElementCount(SCOPE1));
    assertEquals(maxSize + maxSize, mClockFilter.approximateElementSize(SCOPE1));
    for (int i = 0; i <= MAX_AGE; i++) {
      mClockFilter.aging();
    }
    assertEquals(0, mClockFilter.approximateElementCount(SCOPE1));
    assertEquals(0, mClockFilter.approximateElementSize(SCOPE1));
  }

  @Test
  public void testComputeFpp() {
    double epsilon = 1e-6;
    assertEquals(0., mClockFilter.expectedFpp(), epsilon);
    for (int i = 1; i <= EXPECTED_INSERTIONS; i++) {
      mClockFilter.put(i, 1, SCOPE1);
    }
    // although cuckoo filter is nearly full, its fpp is still low
    assertTrue(mClockFilter.expectedFpp() <= 0.01);
  }

  @Test
  public void testConcurrentPut() throws Exception {
    List<Runnable> runnables = new ArrayList<>();
    for (int k = 0; k < DEFAULT_THREAD_AMOUNT; k++) {
      runnables.add(() -> {
        for (int i = 1; i < 500; i++) {
          if (!mClockFilter.mightContainAndResetClock(i)) {
            mClockFilter.put(i, 1, SCOPE1);
          }
          assertTrue(mClockFilter.mightContain(i));
        }
      });
    }
    ConcurrencyUtils.assertConcurrent(runnables, DEFAULT_TIMEOUT_SECONDS);
  }

  @Test
  public void testBackwardMovement() throws Exception {
    // put item 1 into filter,
    // then check whether it exists after every insertion.
    assertTrue(mClockFilter.put(1, 1, SCOPE1));
    assertTrue(mClockFilter.mightContain(1));
    int totalTags = mClockFilter.getNumBuckets() * mClockFilter.getTagsPerBucket();
    List<Runnable> runnables = new ArrayList<>();
    for (int k = 0; k < DEFAULT_THREAD_AMOUNT; k++) {
      runnables.add(() -> {
        for (int i = 2; i <= totalTags * 4; i++) {
          mClockFilter.put(i, 1, SCOPE1);
          // make sure no false negative
          assertTrue(mClockFilter.mightContain(1));
        }
      });
    }
    ConcurrencyUtils.assertConcurrent(runnables, DEFAULT_TIMEOUT_SECONDS);
  }

  @Test
  public void testCountBasedSlingWindowAging() {
    // expect each operation to age once
    long windowSize = (1 << BITS_PER_CLOCK);
    // create a tiny cuckoo filter so that a segment is small enough to make sure
    // inserted item will be opportunistic aged
    long expectedInsertions = 128;
    mClockFilter = ConcurrentClockCuckooFilter.create(Funnels.integerFunnel(), expectedInsertions,
        BITS_PER_CLOCK, BITS_PER_SIZE, BITS_PER_SCOPE, SlidingWindowType.COUNT_BASED, windowSize);
    mClockFilter.put(1, 1, SCOPE1);
    assertEquals(MAX_AGE, mClockFilter.getAge(1));
    for (int i = 1; i <= MAX_AGE; i++) {
      mClockFilter.increaseOperationCount(1);
      // although insertion will fail, opportunistic aging will be carried out
      mClockFilter.put(1, 1, SCOPE1);
      assertEquals(MAX_AGE - i, mClockFilter.getAge(1));
      mClockFilter.aging();
      assertEquals(MAX_AGE - i, mClockFilter.getAge(1));
    }
  }

  @Test
  public void testTimeBasedSlingWindowAging() throws InterruptedException {
    // aging each 1s
    long agingPeriod = Constants.SECOND_MS;
    long windowSize = agingPeriod << BITS_PER_CLOCK;
    // create a tiny cuckoo filter so that a segment is small enough to make sure
    // inserted item will be opportunistic aged
    long expectedInsertions = 128;
    mClockFilter = ConcurrentClockCuckooFilter.create(Funnels.integerFunnel(), expectedInsertions,
        BITS_PER_CLOCK, BITS_PER_SIZE, BITS_PER_SCOPE, SlidingWindowType.TIME_BASED, windowSize);
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(0);
    scheduler.scheduleAtFixedRate(mClockFilter::aging, agingPeriod, agingPeriod, MILLISECONDS);
    // before inserting 1, its two buckets may have been opportunistic aged,
    // so it will probably survived the first aging period
    mClockFilter.put(1, 1, SCOPE1);
    assertEquals(MAX_AGE, mClockFilter.getAge(1));
    for (int i = 1; i <= MAX_AGE; i++) {
      Thread.sleep(100);
      // although insertion will fail, opportunistic aging will be carried out
      mClockFilter.put(1, 1, SCOPE1);
      // if the item survived the first period, it will have an age of `MAX_AGE - i + 1`;
      // if the item do not survived the first period, it will have an age of `MAX_AGE - i`;
      // there is no third case here
      assertTrue(
          mClockFilter.getAge(1) == MAX_AGE - i + 1 || mClockFilter.getAge(1) == MAX_AGE - i);
      Thread.sleep(900);
      assertTrue(
          mClockFilter.getAge(1) == MAX_AGE - i + 1 || mClockFilter.getAge(1) == MAX_AGE - i);
    }
  }
}
