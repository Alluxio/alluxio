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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Tests the {@link IndexedSet} class.
 */
public final class IndexedSetTest {
  private static class Pair {
    private int mInt;
    private long mLong;

    public Pair(int i, long l) {
      mInt = i;
      mLong = l;
    }

    public int intValue() {
      return mInt;
    }

    public long longValue() {
      return mLong;
    }
  }

  private IndexedSet<Pair> mSet;
  private IndexDefinition<Pair, Integer> mNonUniqueIntIndex;
  private IndexDefinition<Pair, Long> mUniqueLongIndex;

  /**
   * Sets up the fields before running a test.
   */
  @Before
  public void before() {
    mNonUniqueIntIndex = new IndexDefinition<Pair, Integer>(false) {
      @Override
      public Integer getFieldValue(Pair o) {
        return o.intValue();
      }
    };

    mUniqueLongIndex = new IndexDefinition<Pair, Long>(true) {
      @Override
      public Long getFieldValue(Pair o) {
        return o.longValue();
      }
    };

    mSet = new IndexedSet<>(mNonUniqueIntIndex, mUniqueLongIndex);
    long l = 0;
    for (int i = 0; i < 3; i++) {
      for (int k = 0; k < 3; k++) {
        mSet.add(new Pair(i, l++));
      }
    }
  }

  /**
   * Tests the {@link IndexedSet#contains(IndexDefinition, Object) } method.
   */
  @Test
  public void UniqueContains() {
    for (int i = 0; i < 3; i++) {
      assertTrue(mSet.contains(mNonUniqueIntIndex, i));
    }
    assertFalse(mSet.contains(mNonUniqueIntIndex, 4));
  }

  /**
   * Tests the {@link IndexedSet#contains(IndexDefinition, Object)} method.
   */
  @Test
  public void NonUniqueContains() {
    for (long l = 0; l < 9; l++) {
      assertTrue(mSet.contains(mUniqueLongIndex, l));
    }
    assertFalse(mSet.contains(mUniqueLongIndex, 9L));
  }

  /**
   * Tests the {@link IndexedSet#getByField(IndexDefinition, Object)} method.
   */
  @Test
  public void nonUniqueGet() {
    for (int i = 0; i < 3; i++) {
      Set<Pair> set = mSet.getByField(mNonUniqueIntIndex, i);
      assertEquals(3, set.size());
      List<Long> longs = new ArrayList<>(set.size());
      for (Pair o : set) {
        longs.add(o.longValue());
      }
      Collections.sort(longs);
      for (int j = 0; j < 3; j++) {
        assertEquals(new Long(i * 3 + j), longs.get(j));
      }
    }
  }

  /**
   * Tests the {@link IndexedSet#getByField(IndexDefinition, Object)} method.
   */
  @Test
  public void uniqueGet() {
    for (int i = 0; i < 9; i++) {
      Set<Pair> set = mSet.getByField(mUniqueLongIndex, (long) i);
      assertEquals(1, set.size());
      assertEquals(i / 3, set.iterator().next().intValue());
    }
  }

  /**
   * Tests that {@link IndexedSet#remove(Object)} method and
   * {@link IndexedSet#contains(IndexDefinition, Object)} method work correctly when trying to
   * remove the data.
   */
  @Test
  public void remove() {
    Pair toRemove = mSet.getFirstByField(mUniqueLongIndex, 1L);
    assertEquals(1, mSet.getByField(mUniqueLongIndex, toRemove.longValue()).size());
    assertEquals(9, mSet.size());
    assertTrue(mSet.remove(toRemove));
    assertEquals(8, mSet.size());

    assertEquals(2, mSet.getByField(mNonUniqueIntIndex, toRemove.intValue()).size());
    assertTrue("Element should be in the NonUniqueIntIndex",
        mSet.contains(mNonUniqueIntIndex, toRemove.intValue()));

    assertEquals(0, mSet.getByField(mUniqueLongIndex, toRemove.longValue()).size());
    assertFalse("Element should not be in the mUniqueLongIndex",
        mSet.contains(mUniqueLongIndex, toRemove.longValue()));

    toRemove = mSet.getFirstByField(mNonUniqueIntIndex, 2);
    assertTrue(mSet.remove(toRemove));
    assertTrue("Element should be in the NonUniqueIntIndex",
        mSet.contains(mNonUniqueIntIndex, toRemove.intValue()));

    toRemove = mSet.getFirstByField(mNonUniqueIntIndex, 2);
    assertTrue(mSet.remove(toRemove));
    assertTrue("Element should be in the NonUniqueIntIndex",
        mSet.contains(mNonUniqueIntIndex, toRemove.intValue()));

    toRemove = mSet.getFirstByField(mNonUniqueIntIndex, 2);
    assertTrue(mSet.remove(toRemove));
    assertFalse("Element should not be in the NonUniqueIntIndex",
        mSet.contains(mNonUniqueIntIndex, toRemove.intValue()));

    toRemove = mSet.getFirstByField(mNonUniqueIntIndex, 2);
    assertTrue(toRemove == null);
  }

  /**
   * Tests the {@link IndexedSet#remove(Object)} method to work correctly when trying to remove a
   * non-existent item.
   */
  @Test
  public void removeNonExist() {
    assertFalse(mSet.remove(new Pair(-1, -1)));
    assertEquals(0, mSet.removeByField(mNonUniqueIntIndex, -1));
    assertEquals(0, mSet.removeByField(mUniqueLongIndex, -1L));
  }

  /**
   * Tests the {@link IndexedSet#removeByField(IndexDefinition, Object)} method.
   */
  @Test
  public void nonUniqueRemoveByField() {
    assertEquals(3, mSet.getByField(mNonUniqueIntIndex, 1).size());
    assertEquals(9, mSet.size());
    assertEquals(3, mSet.removeByField(mNonUniqueIntIndex, 1));
    assertEquals(6, mSet.size());
    assertEquals(0, mSet.getByField(mNonUniqueIntIndex, 1).size());
    assertEquals(3, mSet.getByField(mNonUniqueIntIndex, 0).size());
    assertEquals(3, mSet.getByField(mNonUniqueIntIndex, 2).size());
    for (long l = 3; l < 6; l++) {
      assertEquals(0, mSet.getByField(mUniqueLongIndex, l).size());
    }
  }

  /**
   * Tests the {@link IndexedSet#removeByField(IndexDefinition, Object)} method.
   */
  @Test
  public void uniqueRemoveByField() {
    assertEquals(9, mSet.size());
    assertEquals(1, mSet.removeByField(mUniqueLongIndex, 1L));
    assertEquals(8, mSet.size());
    assertEquals(0, mSet.removeByField(mUniqueLongIndex, 1L));
    assertEquals(8, mSet.size());
    assertEquals(0, mSet.getByField(mUniqueLongIndex, 1L).size());
    assertEquals(1, mSet.getByField(mUniqueLongIndex, 0L).size());
    assertEquals(1, mSet.getByField(mUniqueLongIndex, 2L).size());
    assertEquals(2, mSet.getByField(mNonUniqueIntIndex, 0).size());
  }

  /**
   * Tests that the {@link IndexedSet} works correctly when adding the same object multiple times.
   */
  @Test
  public void addTheSameObjectMultipleTimes() {
    final ExpectedException exception = ExpectedException.none();
    for (int i = 0; i < 3; i++) {
      assertEquals(9, mSet.size());
      assertEquals(3, mSet.getByField(mNonUniqueIntIndex, i).size());
      for (Pair p : mSet.getByField(mNonUniqueIntIndex, i)) {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Adding more than one value to a unique index.");
        mSet.add(p);
      }
      assertEquals(9, mSet.size());
      assertEquals(3, mSet.getByField(mNonUniqueIntIndex, i).size());
    }
    try {
      mSet.add(new Pair(1, 9L));
    } catch (IllegalStateException e) {
      fail();
    }
    assertEquals(10, mSet.size());
    assertEquals(4, mSet.getByField(mNonUniqueIntIndex, 1).size());
  }

  /**
   * Tests that the remove works correctly with the iterator gathered by
   * {@link IndexedSet#iterator()} method.
   */
  @Test
  public void iteratorRemove() {
    Iterator<Pair> it = mSet.iterator();
    assertTrue(it.hasNext());
    final Pair first = it.next();

    Set<Pair> valueSet = mSet.getByField(mNonUniqueIntIndex, first.intValue());
    assertTrue("Element should be in the set", valueSet.contains(first));

    valueSet = mSet.getByField(mUniqueLongIndex, first.longValue());
    assertTrue("Element should be in the set", valueSet.contains(first));

    it.remove();
    valueSet = mSet.getByField(mNonUniqueIntIndex, first.intValue());
    assertFalse("Element should not be in the set", valueSet.contains(first));

    valueSet = mSet.getByField(mUniqueLongIndex, first.longValue());
    assertFalse("Element should not be in the set", valueSet.contains(first));
  }

  /**
   * Tests that foreach works correctly with the iterator gathered by
   * {@link IndexedSet#iterator()} method.
   */
  @Test
  public void iteratorForeach() {
    long removed = 0;
    Iterator<Pair> it = mSet.iterator();
    assertTrue(it.hasNext());
    final Pair first = it.next();

    it.remove();
    removed++;

    while (it.hasNext()) {
      it.next();
      it.remove();
      removed++;
    }
    assertEquals(9L, removed);
    assertEquals(0, mSet.size());

    for (Pair o : mSet) {
      fail();
    }

    long l = 0;
    for (int i = 0; i < 3; i++) {
      assertFalse(mSet.contains(mNonUniqueIntIndex, i));
      for (int k = 0; k < 3; k++) {
        assertFalse(mSet.contains(mUniqueLongIndex, l++));
      }
    }
  }

  /**
   * Tests that next works correctly with the iterator gathered by {@link IndexedSet#iterator()}
   * method.
   */
  @Test
  public void iteratorNext() {
    Iterator<Pair> it = mSet.iterator();
    int intSum = 0;
    int expectedIntSum = 0;
    long longSum = 0;
    long expectedLongSum = 0;

    try {
      long l = 0;
      for (int i = 0; i < 3; i++) {
        for (int k = 0; k < 3; k++) {
          Pair pair = it.next();
          intSum += pair.intValue();
          longSum += pair.longValue();
          expectedIntSum += i;
          expectedLongSum += l++;
        }
      }
    } catch (Exception e) {
      fail();
    }

    assertEquals(expectedIntSum, intSum);
    assertEquals(expectedLongSum, longSum);

    assertFalse(it.hasNext());
  }

  /**
   * Tests that hasNext works correctly with the iterator gathered by {@link IndexedSet#iterator()}
   * method.
   */
  @Test
  public void iteratorhasNext() {
    Iterator<Pair> it = mSet.iterator();
    long size = mSet.size();
    try {
      for (int i = 0; i < size * 2; i++) {
        assertTrue(it.hasNext());
      }
      for (int i = 0; i < 3; i++) {
        for (int k = 0; k < 3; k++) {
          it.next();
        }
      }
      for (int i = 0; i < size; i++) {
        assertFalse(it.hasNext());
      }
    } catch (Exception e) {
      fail();
    }
  }
}
