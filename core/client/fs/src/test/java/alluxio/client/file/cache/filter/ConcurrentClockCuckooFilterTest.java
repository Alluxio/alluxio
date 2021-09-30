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
  private static final int DEFAULT_TIMEOUT_SECONDS = 10;

  private ConcurrentClockCuckooFilter<Integer> clockFilter;

  @Before
  public void beforeTest() {
    init();
  }

  private void init() {
    clockFilter = ConcurrentClockCuckooFilter.create(Funnels.integerFunnel(), EXPECTED_INSERTIONS,
        BITS_PER_CLOCK, BITS_PER_SIZE, BITS_PER_SCOPE);
  }

  @Test
  public void testClockField() {
    for (int i = 1; i <= MAX_AGE; i++) {
      assertTrue(clockFilter.put(i, 1, SCOPE1));
      assertEquals(MAX_AGE, clockFilter.getAge(i));
      clockFilter.aging();
      for (int j = 1; j <= i; j++) {
        assertTrue(clockFilter.mightContain(j));
        assertEquals(MAX_AGE - i + j - 1, clockFilter.getAge(j));
      }
    }
    // fill the filter, and expect reallocation to disturb original position
    for (int i = MAX_AGE + 1; i <= EXPECTED_INSERTIONS; i++) {
      if (!clockFilter.mightContain(i)) {
        clockFilter.put(i, 1, SCOPE1);
      }
    }
    // re-check the age of item 1~15
    for (int i = 1; i <= MAX_AGE; i++) {
      assertTrue(clockFilter.mightContain(i));
      assertEquals(i - 1, clockFilter.getAge(i));
    }
  }

  @Test
  public void testSizeField() {
    int scope1Size = 0;
    for (int i = 1; i <= BITS_PER_SIZE; i++) {
      int s = (1 << i) - 1;
      assertTrue(clockFilter.put(i, s, SCOPE1));
      scope1Size += s;
      assertEquals(scope1Size, clockFilter.getItemSize(SCOPE1));
    }
    // fill the filter, and expect reallocation to disturb original position
    for (int i = MAX_AGE + 1; i <= EXPECTED_INSERTIONS; i++) {
      if (!clockFilter.mightContain(i)) {
        clockFilter.put(i, 1, SCOPE2);
      }
    }
    // delete some items and re-check the size of item 1~15
    assertEquals(scope1Size, clockFilter.getItemSize(SCOPE1));
    for (int i = 1; i <= BITS_PER_SIZE; i++) {
      assertTrue(clockFilter.mightContain(i));
      assertTrue(clockFilter.delete(i));
      assertFalse(clockFilter.mightContain(i));
      scope1Size -= ((1 << i) - 1);
      assertEquals(scope1Size, clockFilter.getItemSize(SCOPE1));
    }
  }

  @Test
  public void testConcurrentPut() throws Exception {
    List<Runnable> runnables = new ArrayList<>();
    for (int k = 0; k < DEFAULT_THREAD_AMOUNT; k++) {
      runnables.add(() -> {
        for (int i = 1; i < 500; i++) {
          if (!clockFilter.mightContainAndResetClock(i)) {
            clockFilter.put(i, 1, SCOPE1);
          }
          assertTrue(clockFilter.mightContain(i));
        }
      });
    }
    ConcurrencyUtils.assertConcurrent(runnables, DEFAULT_TIMEOUT_SECONDS);
  }

  @Test
  public void testBackwardMovement() throws Exception {
    // put item 1 into filter,
    // then check whether it exists after every insertion.
    assertTrue(clockFilter.put(1, 1, SCOPE1));
    assertTrue(clockFilter.mightContain(1));
    int totalTags = clockFilter.numBuckets() * clockFilter.tagsPerBucket();
    List<Runnable> runnables = new ArrayList<>();
    for (int k = 0; k < DEFAULT_THREAD_AMOUNT; k++) {
      runnables.add(() -> {
        for (int i = 2; i <= totalTags * 4; i++) {
          clockFilter.put(i, 1, SCOPE1);
          // make sure no false negative
          assertTrue(clockFilter.mightContain(1));
        }
      });
    }
    ConcurrencyUtils.assertConcurrent(runnables, DEFAULT_TIMEOUT_SECONDS);
  }
}
