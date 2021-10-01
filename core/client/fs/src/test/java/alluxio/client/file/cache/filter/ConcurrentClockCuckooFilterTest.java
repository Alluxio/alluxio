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

package alluxio.client.file.cache.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.test.util.ConcurrencyUtils;

import com.google.common.hash.Funnels;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ConcurrentClockCuckooFilterTest {
  private static final int EXPECTED_INSERTIONS = Constants.KB;
  private static final int BITS_PER_CLOCK = 4;
  private static final int MAX_AGE = (1 << BITS_PER_CLOCK) - 1;
  private static final int BITS_PER_SIZE = 20;
  private static final int BITS_PER_SCOPE = 8;

  private static final ScopeInfo SCOPE1 = new ScopeInfo("table1");
  private static final ScopeInfo SCOPE2 = new ScopeInfo("table2");

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
      assertEquals(scope1Size, mClockFilter.getItemSize(SCOPE1));
    }
    // fill the filter, and expect reallocation to disturb original position
    for (int i = MAX_AGE + 1; i <= EXPECTED_INSERTIONS; i++) {
      if (!mClockFilter.mightContain(i)) {
        mClockFilter.put(i, 1, SCOPE2);
      }
    }
    // delete some items and re-check the size of item 1~15
    assertEquals(scope1Size, mClockFilter.getItemSize(SCOPE1));
    for (int i = 1; i <= BITS_PER_SIZE; i++) {
      assertTrue(mClockFilter.mightContain(i));
      assertTrue(mClockFilter.delete(i));
      assertFalse(mClockFilter.mightContain(i));
      scope1Size -= ((1 << i) - 1);
      assertEquals(scope1Size, mClockFilter.getItemSize(SCOPE1));
    }
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
}
