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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A set which delegates all methods to another set.
 *
 * @param <T> the type of element stored in the set
 */
public class DelegatingSet<T> implements Set<T> {
  private final Set<T> mDelegate;

  /**
   * @param delegate the delegate to delegate to
   */
  public DelegatingSet(Set<T> delegate) {
    mDelegate = delegate;
  }

  @Override
  public int size() {
    return mDelegate.size();
  }

  @Override
  public boolean isEmpty() {
    return mDelegate.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return mDelegate.contains(o);
  }

  @Override
  public Iterator<T> iterator() {
    return mDelegate.iterator();
  }

  @Override
  public Object[] toArray() {
    return mDelegate.toArray();
  }

  @Override
  public <T1> T1[] toArray(T1[] a) {
    return mDelegate.toArray(a);
  }

  @Override
  public boolean add(T t) {
    return mDelegate.add(t);
  }

  @Override
  public boolean remove(Object o) {
    return mDelegate.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return mDelegate.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    return mDelegate.addAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return mDelegate.retainAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return mDelegate.removeAll(c);
  }

  @Override
  public void clear() {
    mDelegate.clear();
  }

  @Override
  public boolean equals(Object o) {
    return mDelegate.equals(o);
  }

  @Override
  public int hashCode() {
    return mDelegate.hashCode();
  }

  @Override
  public Spliterator<T> spliterator() {
    return mDelegate.spliterator();
  }

  @Override
  public boolean removeIf(Predicate<? super T> filter) {
    return mDelegate.removeIf(filter);
  }

  @Override
  public Stream<T> stream() {
    return mDelegate.stream();
  }

  @Override
  public Stream<T> parallelStream() {
    return mDelegate.parallelStream();
  }

  @Override
  public void forEach(Consumer<? super T> action) {
    mDelegate.forEach(action);
  }
}
