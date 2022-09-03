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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * A {@code CloseableIterator<T>} is an iterator which requires cleanup when it is no longer in use.
 *
 * When the user creates {@code CloseableIterator}, the {@code closeResource()} method needs to be
 * implemented to define how the resource can be closed.
 * The iterator is not necessarily {@code Closeable} so we do not provide a default implementation
 * for {@code closeResource()}.
 *
 * Users are required to use the factory methods instead of the constructors to create instances.
 *
 * @param <T> the type of elements returned by the iterator
 */
public abstract class CloseableIterator<T> extends CloseableResource<Iterator<T>>
    implements Iterator<T> {
  Iterator<T> mIter;

  /**
   * Creates a {@link CloseableIterator} wrapper around the given iterator. This iterator will
   * be returned by the {@link CloseableIterator#get()} method.
   *
   * @param iterator the resource to wrap
   */
  CloseableIterator(Iterator<T> iterator) {
    super(iterator);
    mIter = iterator;
  }

  private Iterator<T> getIterator() {
    return mIter;
  }

  @Override
  public boolean hasNext() {
    return mIter.hasNext();
  }

  @Override
  public T next() {
    return mIter.next();
  }

  /**
   * Wrap around an iterator with a resource to close.
   * How to close the resource is defined by the caller in the lambda expression.
   *
   * @param iterator the iterator to wrap around
   * @param closeAction the callback to properly clean up the resource
   * @param <T> the type of the iterable
   * @return the closeable iterator
   */
  public static <T> CloseableIterator<T> create(
      Iterator<? extends T> iterator, Consumer<Void> closeAction) {
    return new CloseableIterator(iterator) {
      @Override
      public void closeResource() {
        closeAction.accept(null);
      }
    };
  }

  /**
   * Wraps around an iterator with no resource to close.
   *
   * @param iterator the iterator to wrap around
   * @param <T> the type of the iterable
   * @return the closeable iterator
   */
  public static <T> CloseableIterator<T> noopCloseable(Iterator<? extends T> iterator) {
    // There is no resource to close
    return new CloseableIterator(iterator) {
      @Override
      public void closeResource() {
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
   * Concatenates iterators.
   *
   * @param iterators a list of iterators
   * @param <T> type of iterator
   * @return a concatenated iterator that iterate over all elements from each of the child iterators
   */
  public static <T> CloseableIterator<T> concat(
      List<CloseableIterator<T>> iterators) {
    // Concat the iterators
    Iterator<T> it =
        Iterators.concat(iterators.stream().map(CloseableIterator::getIterator).iterator());
    // Register the resources
    final Closer closer = Closer.create();
    iterators.forEach(closer::register);
    return new CloseableIterator<T>(it) {
      @Override
      public void closeResource() {
        try {
          closer.close();
        } catch (IOException e) {
          throw new RuntimeException("Failed to close iterator", e);
        }
      }
    };
  }

  /**
   * Consumes the iterator, closes it, and returns its size.
   * @param iter the iter to get the size of
   * @return the size of the iter
   */
  @VisibleForTesting
  public static int size(CloseableIterator<?> iter) {
    int size = Iterators.size(iter);
    iter.close();
    return size;
  }
}
