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

package alluxio.resource;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tests for {@link CloseableIterator} functions.
 */
public final class CloseableIteratorTest {
  @Test
  public void concatTwo() {
    ArrayList<Integer> list1 = Lists.newArrayList(1, 2);
    ArrayList<Integer> list2 = Lists.newArrayList(3, 4, 5);
    TestIterator iterator1 = new TestIterator(list1);
    TestIterator iterator2 = new TestIterator(list2);
    CloseableIterator<Integer> iterator3 = CloseableIterator.concat(iterator1, iterator2);
    Iterators.elementsEqual(Iterators.concat(list1.iterator(), list2.iterator()), iterator3.get());
    iterator3.close();
    Assert.assertTrue(iterator1.isClosed());
    Assert.assertTrue(iterator2.isClosed());
  }

  @Test
  public void concatList() {
    ArrayList<Integer> list1 = Lists.newArrayList(1, 2);
    ArrayList<Integer> list2 = Lists.newArrayList(3, 4, 5);
    ArrayList<Integer> list3 = Lists.newArrayList(0);
    TestIterator iterator1 = new TestIterator(list1);
    TestIterator iterator2 = new TestIterator(list2);
    TestIterator iterator3 = new TestIterator(list3);
    CloseableIterator<Integer> iterator4 = CloseableIterator.concat(
        Lists.newArrayList(iterator1, iterator2, iterator3));
    Iterators.elementsEqual(Iterators.concat(list1.iterator(), list2.iterator(), list3.iterator()),
        iterator4.get());
    iterator4.close();
    Assert.assertTrue(iterator1.isClosed());
    Assert.assertTrue(iterator2.isClosed());
    Assert.assertTrue(iterator3.isClosed());
  }

  @Test
  public void noopCloseable() {
    Iterator<Integer> iterator = Lists.newArrayList(1, 2).iterator();
    CloseableIterator<Integer> closeable = CloseableIterator
        .noopCloseable(Lists.newArrayList(1, 2).iterator());
    Iterators.elementsEqual(iterator, closeable.get());
  }

  static class TestIterator extends CloseableIterator<Integer> {
    private boolean mClosed;

    public TestIterator(List<Integer> list) {
      super(list.iterator());
    }

    @Override
    public void close() {
      mClosed = true;
    }

    public boolean isClosed() {
      return mClosed;
    }
  }
}
