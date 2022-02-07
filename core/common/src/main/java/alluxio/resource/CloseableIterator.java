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
import java.util.concurrent.Callable;

/**
 * A {@code CloseableIterator<T>} is an iterator which requires cleanup when it is no longer in use.
 *
 * @param <T> the type of elements returned by the iterator
 */
public abstract class CloseableIterator<T, R> extends CloseableResource<R> implements Iterator<T> {
  Iterator<T> mIter;
  Callable<Void> mCloseAction;

//  /**
//   * Creates a {@link CloseableIterator} wrapper around the given iterator. This iterator will
//   * be returned by the {@link CloseableIterator#get()} method.
//   *
//   * @param iterator the resource to wrap
//   */
//  CloseableIterator(Iterator<T> iterator) {
//    super(iterator);
//    mIter = iterator;
//  }

  // TODO(jiacheng): can you make the generic recursive?
  CloseableIterator(CloseableIterator<T, R> closeableIterator) {
    super(closeableIterator.get());
    mIter = closeableIterator;
  }

  CloseableIterator(Iterator<T> iterator, R resource) {
    super(resource);
    mIter = iterator;
  }

  Iterator<T> getIterator() {
    return mIter;
  }

  // TODO(jiacheng): avoid the abstract class and anonymous inner classes
  public static <T, R> CloseableIterator<T, R> create(Iterator<T> iterator, Callable<Void> closeAction) {
    return new CloseableIterator<T, R>(iterator, null) {
      @Override
      public void closeResource() {
        try {
          closeAction.call();
        } catch (Exception e) {
          System.err.println(e);
        }
      }

      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public T next() {
        return null;
      }
    };
  }


  /**
   * Wrap around an iterator with no resource to close.
   *
   * @param iterator the iterator to wrap around
   * @param <T> the type of the iterable
   * @return the closeable iterator
   */
  public static <T, R> CloseableIterator<T, R> noopCloseable(Iterator<T> iterator) {
    // TODO(jiacheng): is there a way to avoid null?
    // There is no resource to close
    return new CloseableIterator<T, R>(iterator, null) {
      @Override
      public boolean hasNext() {
        return mIter.hasNext();
      }

      @Override
      public T next() {
        return mIter.next();
      }

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
  public static <T, R> CloseableIterator<T, Closer> concat(CloseableIterator<T, R> a, CloseableIterator<T, R> b) {
    return concat(Lists.newArrayList(a, b));
  }

  /**
   * Concatenate iterators.
   *
   * @param iterators a list of iterators
   * @param <T> type of iterator
   * @return a concatenated iterator that iterate over all elements from each of the child iterators
   */
  public static <T, R> CloseableIterator<T, Closer> concat(
      List<CloseableIterator<T, R>> iterators) {
//    Iterator<? extends T> it =
//        Iterators.concat(iterators.stream().map(CloseableIterator::get).iterator());
//    Iterator<T> concatIter = Iterators.concat(it);

    // Concat the iterators
    Iterator<T> it =
            Iterators.concat(iterators.stream().map(CloseableIterator::getIterator).iterator());
    // Register the resources
    Closer c = Closer.create();
    iterators.forEach(c::register);
    // TODO(jiacheng): find a good way of using wildcard generics
    return new CloseableIterator<T, Closer>(it, c) {
      @Override
      public boolean hasNext() {
        return mIter.hasNext();
      }

      @Override
      public T next() {
        return mIter.next();
      }

      @Override
      public void closeResource() {
        try {
          c.close();
        } catch (IOException e) {
          throw new RuntimeException("Failed to close iterator", e);
        }
      }
    };
  }
}
