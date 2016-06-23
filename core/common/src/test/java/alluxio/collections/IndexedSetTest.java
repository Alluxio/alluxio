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

import org.junit.Assert;
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
public class IndexedSetTest {
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
  private IndexDefinition<Pair> mNonUniqueIntIndex;
  private IndexDefinition<Pair> mUniqueLongIndex;

  /**
   * Sets up the fields before running a test.
   */
  @Before
  public void before() {
    mNonUniqueIntIndex = new IndexDefinition<Pair>(false) {
      @Override
      public Object getFieldValue(Pair o) {
        return o.intValue();
      }
    };

    mUniqueLongIndex = new IndexDefinition<Pair>(true) {
      @Override
      public Object getFieldValue(Pair o) {
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
  public void UniqueContainsTest() {
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(mSet.contains(mNonUniqueIntIndex, i));
    }
    Assert.assertFalse(mSet.contains(mNonUniqueIntIndex, 4));
  }

  /**
   * Tests the {@link IndexedSet#contains(IndexDefinition, Object)} method.
   */
  @Test
  public void NonUniqueContainsTest() {
    for (long l = 0; l < 9; l++) {
      Assert.assertTrue(mSet.contains(mUniqueLongIndex, l));
    }
    Assert.assertFalse(mSet.contains(mUniqueLongIndex, 9L));
  }

  /**
   * Tests the {@link IndexedSet#getByField(IndexDefinition, Object)} method.
   */
  @Test
  public void nonUniqueGetTest() {
    for (int i = 0; i < 3; i++) {
      Set<Pair> set = mSet.getByField(mNonUniqueIntIndex, i);
      Assert.assertEquals(3, set.size());
      List<Long> longs = new ArrayList<>(set.size());
      for (Pair o : set) {
        longs.add(o.longValue());
      }
      Collections.sort(longs);
      for (int j = 0; j < 3; j++) {
        Assert.assertEquals(new Long(i * 3 + j), longs.get(j));
      }
    }
  }

  /**
   * Tests the {@link IndexedSet#getByField(IndexDefinition, Object)} method.
   */
  @Test
  public void uniqueGetTest() {
    for (int i = 0; i < 9; i++) {
      Set<Pair> set = mSet.getByField(mUniqueLongIndex, i);
      Assert.assertEquals(0, set.size()); // i is integer, must be in the same type
      set = mSet.getByField(mUniqueLongIndex, (long) i);
      Assert.assertEquals(1, set.size());
      Assert.assertEquals(i / 3, set.iterator().next().intValue());
    }
  }

  /**
   * Tests the {@link IndexedSet#remove(Object)} method.
   */
  @Test
  public void removeTest() {
    Pair toRemove = mSet.getFirstByField(mUniqueLongIndex, 1L);
    Assert.assertEquals(1, mSet.getByField(mUniqueLongIndex, toRemove.longValue()).size());
    Assert.assertEquals(9, mSet.size());
    Assert.assertTrue(mSet.remove(toRemove));
    Assert.assertEquals(8, mSet.size());
    Assert.assertEquals(2, mSet.getByField(mNonUniqueIntIndex, toRemove.intValue()).size());
    Assert.assertEquals(0, mSet.getByField(mUniqueLongIndex, toRemove.longValue()).size());
  }

  /**
   * Tests the {@link IndexedSet#remove(Object)} method to work correctly when trying to remove a
   * non-existent item.
   */
  @Test
  public void removeNonExistTest() {
    Assert.assertFalse(mSet.remove(new Pair(-1, -1)));
    Assert.assertEquals(0, mSet.removeByField(mNonUniqueIntIndex, -1));
    Assert.assertEquals(0, mSet.removeByField(mUniqueLongIndex, -1L));
  }

  /**
   * Tests the {@link IndexedSet#removeByField(IndexDefinition, Object)} method.
   */
  @Test
  public void nonUniqueRemoveByFieldTest() {
    Assert.assertEquals(3, mSet.getByField(mNonUniqueIntIndex, 1).size());
    Assert.assertEquals(9, mSet.size());
    Assert.assertEquals(3, mSet.removeByField(mNonUniqueIntIndex, 1));
    Assert.assertEquals(6, mSet.size());
    Assert.assertEquals(0, mSet.getByField(mNonUniqueIntIndex, 1).size());
    Assert.assertEquals(3, mSet.getByField(mNonUniqueIntIndex, 0).size());
    Assert.assertEquals(3, mSet.getByField(mNonUniqueIntIndex, 2).size());
    for (long l = 3; l < 6; l++) {
      Assert.assertEquals(0, mSet.getByField(mUniqueLongIndex, l).size());
    }
  }

  /**
   * Tests the {@link IndexedSet#removeByField(IndexDefinition, Object)} method.
   */
  @Test
  public void uniqueRemoveByFieldTest() {
    Assert.assertEquals(9, mSet.size());
    Assert.assertEquals(1, mSet.removeByField(mUniqueLongIndex, 1L));
    Assert.assertEquals(8, mSet.size());
    Assert.assertEquals(0, mSet.removeByField(mUniqueLongIndex, 1L));
    Assert.assertEquals(8, mSet.size());
    Assert.assertEquals(0, mSet.getByField(mUniqueLongIndex, 1L).size());
    Assert.assertEquals(1, mSet.getByField(mUniqueLongIndex, 0L).size());
    Assert.assertEquals(1, mSet.getByField(mUniqueLongIndex, 2L).size());
    Assert.assertEquals(2, mSet.getByField(mNonUniqueIntIndex, 0).size());
  }

  /**
   * Tests that the {@link IndexedSet} works correctly when adding the same object multiple times.
   */
  @Test
  public void addTheSameObjectMultipleTimesTest() {
    final ExpectedException exception = ExpectedException.none();
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(9, mSet.size());
      Assert.assertEquals(3, mSet.getByField(mNonUniqueIntIndex, i).size());
      for (Pair p : mSet.getByField(mNonUniqueIntIndex, i)) {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Adding more than one value to a unique index.");
        mSet.add(p);
      }
      Assert.assertEquals(9, mSet.size());
      Assert.assertEquals(3, mSet.getByField(mNonUniqueIntIndex, i).size());
    }
    try {
      mSet.add(new Pair(1 , 9L));
    } catch (IllegalStateException e) {
      Assert.assertTrue(true);
    }
    Assert.assertEquals(10, mSet.size());
    Assert.assertEquals(4, mSet.getByField(mNonUniqueIntIndex, 1).size());
  }

  /**
   * Tests that the remove works correctly with the iterator gathered by
   * {@link IndexedSet#iterator()} method.
   */
  @Test
  public void iteratorRemoveTest() {
    Iterator<Pair> it =  mSet.iterator();
    Assert.assertTrue(it.hasNext());
    final Pair first = it.next();
    Set<Pair> allWithSameIntValue = mSet.getByField(mNonUniqueIntIndex, first.intValue());
    Assert.assertTrue("Element should be in the set", allWithSameIntValue.contains(first));
    it.remove();
    allWithSameIntValue = mSet.getByField(mNonUniqueIntIndex, first.intValue());
    Assert.assertFalse("Element should not be in the set", allWithSameIntValue.contains(first));
  }
}
