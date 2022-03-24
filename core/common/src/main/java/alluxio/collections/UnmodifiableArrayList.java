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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * This class provides an unmodifiable List proxy for an underlying array.
 * In other words, this helps you use the array like a List, but not being able to modify it.
 *
 * The difference to {@code Collections.unmodifiableList(Arrays.asList(array))} is,
 * in some Java implementations like adoptjdk, Arrays.asList() copies the array with
 * {@code new ArrayList<>(array)} so it is not performant.
 *
 * Use this when you just want to return an unmodifiable view of the array.
 *
 * Create an instance with the constructor and get the underlying array with {@link #toArray()}.
 */
public class UnmodifiableArrayList<T> implements List<T> {
  T[] mElements;

  /**
   * @param elements the underlying array
   */
  public UnmodifiableArrayList(T[] elements) {
    Preconditions.checkNotNull(elements);
    mElements = elements;
  }

  @Override
  public int size() {
    return mElements.length;
  }

  @Override
  public boolean isEmpty() {
    return mElements == null || mElements.length == 0;
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) >= 0;
  }

  @Override
  public Iterator<T> iterator() {
    return Iterators.forArray(mElements);
  }

  @Override
  public Object[] toArray() {
    return mElements;
  }

  @Override
  public <T1> T1[] toArray(T1[] a) {
    // An exception will be thrown if the target type is not convertible
    return (T1[]) mElements;
  }

  @Override
  public boolean add(T t) {
    throw new UnsupportedOperationException(
        "modification is not supported in UnmodifiableArrayList");
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException(
        "modification is not supported in UnmodifiableArrayList");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException(
        "modification is not supported in UnmodifiableArrayList");
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    throw new UnsupportedOperationException(
        "modification is not supported in UnmodifiableArrayList");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException(
        "modification is not supported in UnmodifiableArrayList");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException(
        "modification is not supported in UnmodifiableArrayList");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException(
        "modification is not supported in UnmodifiableArrayList");
  }

  @Override
  public T get(int index) {
    return mElements[index];
  }

  @Override
  public T set(int index, T element) {
    throw new UnsupportedOperationException(
        "modification is not supported in UnmodifiableArrayList");
  }

  @Override
  public void add(int index, T element) {
    throw new UnsupportedOperationException(
        "modification is not supported in UnmodifiableArrayList");
  }

  @Override
  public T remove(int index) {
    throw new UnsupportedOperationException(
        "modification is not supported in UnmodifiableArrayList");
  }

  @Override
  public int lastIndexOf(Object o) {
    return 0;
  }

  @Override
  public ListIterator<T> listIterator() {
    return new ListItr(0);
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    if (index < 0 || index > mElements.length)
      throw new IndexOutOfBoundsException("Index: "+index);
    return new ListItr(index);
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    throw new UnsupportedOperationException(
        "subList is not supported in UnmodifiableArrayList, use the underlying array instead");
  }

  @Override
  public int indexOf(Object o) {
    if (o == null) {
      for (int i = 0; i < mElements.length; i++)
        if (mElements[i]==null) {
          return i;
        }
    } else {
      for (int i = 0; i < mElements.length; i++)
        if (o.equals(mElements[i])) {
          return i;
        }
    }
    return -1;
  }

  private class Itr implements Iterator<T> {
    int cursor;       // index of next element to return
    int lastRet = -1; // index of last element returned; -1 if no such

    Itr() {}

    public boolean hasNext() {
      return cursor != mElements.length;
    }

    @Override
    public T next() {
      int i = cursor;
      if (i >= mElements.length)
        throw new NoSuchElementException();
      cursor = i + 1;
      return mElements[lastRet = i];
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "modification is not supported in UnmodifiableArrayList");
    }

    @Override
    public void forEachRemaining(Consumer<? super T> consumer) {
      Objects.requireNonNull(consumer);
      final int size = mElements.length;
      int i = cursor;
      if (i >= size) {
        return;
      }
      while (i != size) {
        consumer.accept(mElements[i++]);
      }
      // update once at end of iteration to reduce heap write traffic
      cursor = i;
      lastRet = i - 1;
    }
  }

  private class ListItr extends Itr implements ListIterator<T> {
    ListItr(int index) {
      super();
      cursor = index;
    }

    public boolean hasPrevious() {
      return cursor != 0;
    }

    public int nextIndex() {
      return cursor;
    }

    public int previousIndex() {
      return cursor - 1;
    }

    public T previous() {
      int i = cursor - 1;
      if (i < 0)
        throw new NoSuchElementException();
      if (i >= mElements.length)
        throw new ConcurrentModificationException();
      cursor = i;
      return mElements[lastRet = i];
    }

    public void set(T e) {
      throw new UnsupportedOperationException(
          "modification is not supported in UnmodifiableArrayList");
    }

    public void add(T e) {
      throw new UnsupportedOperationException(
          "modification is not supported in UnmodifiableArrayList");
    }
  }
}
