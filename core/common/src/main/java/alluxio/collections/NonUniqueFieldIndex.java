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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A class representing a non-unique index. A non-unique index is
 * an index where an index value can map to one or more objects.
 *
 * @param <T> type of objects in this {@link IndexedSet}
 */
class NonUniqueFieldIndex<T> implements FieldIndex<T> {
  private final IndexDefinition<T> mIndexDefinition;
  private final ConcurrentHashMap<Object, ConcurrentHashSet<T>> mIndexMap;

  /**
   * Constructs a new {@link NonUniqueFieldIndex} instance.
   */
  public NonUniqueFieldIndex(IndexDefinition<T> indexDefinition) {
    mIndexMap = new ConcurrentHashMap<>(8, 0.95f, 8);
    mIndexDefinition = indexDefinition;
  }

  @Override
  public void add(T object) {
    Object fieldValue = mIndexDefinition.getFieldValue(object);

    ConcurrentHashSet<T> objSet;

    while (true) {
      objSet = mIndexMap.get(fieldValue);
      // If there is no object set for the current value, creates a new one.
      while (objSet == null) {
        mIndexMap.putIfAbsent(fieldValue, new ConcurrentHashSet<T>());
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
  }

  @Override
  public boolean remove(T object) {
    boolean res = false;
    Object fieldValue = mIndexDefinition.getFieldValue(object);
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
  public boolean contains(Object value) {
    return mIndexMap.containsKey(value);
  }

  @Override
  public Set<T> getByField(Object value) {
    Set<T> set = mIndexMap.get(value);
    return set == null ? Collections.<T>emptySet() : set;
  }

  @Override
  public T getFirst(Object value) {
    Set<T> all = mIndexMap.get(value);
    return all == null ? null : Iterables.getFirst(all, null);
  }
}
