/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.next;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class IndexedSetTest {
  class Pair {
    // mInt is private, mDouble is public, this is deliberate to assure both private and public
    // fields can be accessed.
    private int mInt;
    public double mDouble;

    public Pair(int i, double d) {
      mInt = i;
      mDouble = d;
    }

    public int intValue() {
      return mInt;
    }
  }

  private IndexedSet<Pair> mSet;

  @Before
  public void before() {
    mSet = new IndexedSet<Pair>("mInt", "mDouble");
    for (int i = 0; i < 3; i ++) {
      for (double d = 0; d < 3.0; d ++) {
        mSet.add(new Pair(i, d));
      }
    }
  }

  @Test
  public void containsTest() {
    for (int i = 0; i < 3; i ++) {
      Assert.assertTrue(mSet.contains("mInt", i));
    }
    Assert.assertFalse(mSet.contains("mInt", 4));
    for (double d = 0; d < 3.0; d ++) {
      Assert.assertTrue(mSet.contains("mDouble", d));
    }
    Assert.assertFalse(mSet.contains("mDouble", 2.9));
  }

  @Test
  public void getTest() {
    for (int i = 0; i < 3; i ++) {
      Set<Pair> set = mSet.getByField("mInt", i);
      Assert.assertEquals(3, set.size());
      List<Double> doubles = new ArrayList<Double>(set.size());
      for (Pair o : set) {
        doubles.add(o.mDouble);
      }
      Collections.sort(doubles);
      for (int j = 0; j < 3; j ++) {
        Assert.assertEquals(new Double(j), doubles.get(j));
      }

      set = mSet.getByField("mDouble", i);
      Assert.assertEquals(0, set.size()); // i is integer, must be in the same type
      set = mSet.getByField("mDouble", (double) i);
      Assert.assertEquals(3, set.size());
      List<Integer> ints = new ArrayList<Integer>(set.size());
      for (Pair o : set) {
        ints.add(o.intValue());
      }
      Collections.sort(ints);
      for (int j = 0; j < 3; j ++) {
        Assert.assertEquals(new Integer(j), ints.get(j));
      }
    }
  }

  @Test
  public void removeTest() {
    Pair toRemove = mSet.all().iterator().next();
    Assert.assertEquals(3, mSet.getByField("mDouble", toRemove.mDouble).size());
    Assert.assertTrue(mSet.remove(toRemove));
    Assert.assertEquals(2, mSet.getByField("mDouble", toRemove.mDouble).size());
  }

  @Test
  public void removeByFieldTest() {
    Assert.assertEquals(3, mSet.getByField("mInt", 1).size());
    Assert.assertTrue(mSet.removeByField("mInt", 1));
    Assert.assertEquals(0, mSet.getByField("mInt", 1).size());
  }
}
