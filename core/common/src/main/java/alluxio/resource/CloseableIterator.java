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
import com.google.common.io.Closer;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * A {@code CloseableIterator<T>} is an iterator which requires cleanup when it is no longer in use.
 *
 * @param <T> the type of elements returned by the iterator
 */
public abstract class CloseableIterator<T> extends CloseableResource<Iterator<T>> {
  /**
   * Creates a {@link CloseableIterator} wrapper around the given iterator. This iterator will
   * be returned by the {@link CloseableIterator#get()} method.
   *
   * @param iterator the resource to wrap
   */
  public CloseableIterator(Iterator<T> iterator) {
    super(iterator);
  }

  /**
   * Wrap around an iterator with no resource to close.
   *
   * @param iterator the iterator to wrap around
   * @param <T> the type of the iterable
   * @return the closeable iterator
   */
  public static <T> CloseableIterator<T> noopCloseable(Iterator<? extends T> iterator) {
    return new CloseableIterator(iterator) {
      @Override
      public void close() {
        // no-op
      }
    };
  }

  /**
   * Combines two iterators into a single iterator.
   *
   * @param a an iterator
   * @param b another iterator
   * @param <T> type of iterator
   * @return the concatenated iterator
   */
  public static <T> CloseableIterator<T> concat(CloseableIterator<T> a, CloseableIterator<T> b) {
    return concat(Lists.newArrayList(a, b));
  }

  /**
   * Concatenate iterators.
   *
   * @param iterators a list of iterators
   * @param <T> type of iterator
   * @return a concatenated iterator that iterate over all elements from each of the child iterators
   */
  public static <T> CloseableIterator<T> concat(
      List<CloseableIterator<T>> iterators) {
    Iterator<? extends T> it = Iterators.concat(iterators.stream().map(CloseableIterator::get)
        .iterator());
    return new CloseableIterator<T>(Iterators.concat(it)) {
      @Override
      public void close() {
        Closer c = Closer.create();
        iterators.forEach(c::register);
        try {
          c.close();
        } catch (IOException e) {
          throw new RuntimeException("Failed to close iterator", e);
        }
      }
    };
  }
}
