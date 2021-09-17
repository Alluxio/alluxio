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

import com.google.common.collect.Iterables;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class representing a non-unique index. A non-unique index is an index where an index value can
 * map to one or more objects.
 *
 * @param <T> type of objects in this index
 * @param <V> type of the field used for indexing
 */
@ThreadSafe
public class NonUniqueFieldIndex<T, V> implements FieldIndex<T, V> {
  private final IndexDefinition<T, V> mIndexDefinition;
  private final ConcurrentHashMap<V, ConcurrentHashSet<T>> mIndexMap;

  /**
   * Constructs a new {@link NonUniqueFieldIndex} instance.
   *
   * @param indexDefinition definition of index
   */
  public NonUniqueFieldIndex(IndexDefinition<T, V> indexDefinition) {
    mIndexMap = new ConcurrentHashMap<>(8, 0.95f, 8);
    mIndexDefinition = indexDefinition;
  }

  @Override
  public boolean add(T object) {
    V fieldValue = mIndexDefinition.getFieldValue(object);

    ConcurrentHashSet<T> objSet;

    while (true) {
      objSet = mIndexMap.get(fieldValue);
      // If there is no object set for the current value, creates a new one.
      while (objSet == null) {
        mIndexMap.putIfAbsent(fieldValue, new ConcurrentHashSet<>());
        objSet = mIndexMap.get(fieldValue);
      }

      synchronized (objSet) {
        if (objSet != mIndexMap.get(fieldValue)) {
          continue;
        }
        // Adds the value to the object set.
        objSet.add(object);
        break;
      }
    }
    return true;
  }

  @Override
  public boolean remove(T object) {
    boolean res = false;
    V fieldValue = mIndexDefinition.getFieldValue(object);
    ConcurrentHashSet<T> objSet = mIndexMap.get(fieldValue);
    if (objSet != null) {
      synchronized (objSet) {
        if (objSet != mIndexMap.get(fieldValue)) {
          return false;
        }
        res = objSet.remove(object);
        if (objSet.isEmpty()) {
          mIndexMap.remove(fieldValue, objSet);
        }
      }
    }
    return res;
  }

  @Override
  public void clear() {
    mIndexMap.clear();
  }

  @Override
  public boolean containsField(V fieldValue) {
    return mIndexMap.containsKey(fieldValue);
  }

  @Override
  public boolean containsObject(T object) {
    V fieldValue = mIndexDefinition.getFieldValue(object);
    ConcurrentHashSet<T> set = mIndexMap.get(fieldValue);

    if (set == null) {
      return false;
    }
    return set.contains(object);
  }

  @Override
  public Set<T> getByField(V value) {
    Set<T> set = mIndexMap.get(value);
    return set == null ? Collections.<T>emptySet() : set;
  }

  @Override
  public T getFirst(V value) {
    Set<T> all = mIndexMap.get(value);
    return all == null ? null : Iterables.getFirst(all, null);
  }

  @Override
  public Iterator<T> iterator() {
    return new NonUniqueFieldIndexIterator();
  }

  @Override
  public int size() {
    int totalSize = 0;
    for (ConcurrentHashSet<T> innerSet : mIndexMap.values()) {
      totalSize += innerSet.size();
    }
    return totalSize;
  }

  /**
   * Specialized iterator for {@link NonUniqueFieldIndex}.
   *
   * This is needed to support consistent removal from the set and the indices.
   */
  private class NonUniqueFieldIndexIterator implements Iterator<T> {
    /**
     * Iterator of {@link NonUniqueFieldIndex#mIndexMap}. This iterator keeps track of the
     * inner set which is under iteration.
     */
    private final Iterator<ConcurrentHashSet<T>> mIndexIterator;
    /**
     * Iterator inside each inner set. This iterator keeps track of the objects.
     */
    private Iterator<T> mObjectIterator;
    /**
     * Keeps track of current object. It is used to do the remove.
     */
    private T mObject;

    public NonUniqueFieldIndexIterator() {
      mIndexIterator = mIndexMap.values().iterator();
      mObjectIterator = null;
      mObject = null;
    }

    @Override
    public boolean hasNext() {
      if (mObjectIterator != null && mObjectIterator.hasNext()) {
        return true;
      }
      while (mIndexIterator.hasNext()) {
        mObjectIterator = mIndexIterator.next().iterator();
        if (mObjectIterator.hasNext()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public T next() {
      while (mObjectIterator == null || !mObjectIterator.hasNext()) {
        mObjectIterator = mIndexIterator.next().iterator();
      }

      final T next = mObjectIterator.next();
      mObject = next;
      return next;
    }

    @Override
    public void remove() {
      if (mObject != null) {
        NonUniqueFieldIndex.this.remove(mObject);
        mObject = null;
      } else {
        throw new IllegalStateException("next() was not called before remove()");
      }
    }
  }
}
