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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An interface extending {@link FieldIndex}, represents a unique index. A unique index is an index
 * where each index value only maps to one object.
 *
 * @param <T> type of objects in this {@link IndexedSet}
 */
class UniqueFieldIndex<T> implements FieldIndex<T> {
  private final IndexDefinition.Abstracter mAbstracter;
  private ConcurrentHashMap<Object, T> mIndexMap;

  /**
   * Constructs a new {@link UniqueFieldIndex} instance.
   */
  public UniqueFieldIndex(IndexDefinition.Abstracter abstracter) {
    mIndexMap = new ConcurrentHashMap<Object, T>(8, 0.95f, 8);
    mAbstracter = abstracter;
  }

  /**
   * Gets the object with the specified field value - internal function.
   *
   * @param value the field value
   * @return the object with the specified field value
   */
  public T get(Object value) {
    return mIndexMap.get(value);
  }

  @Override
  public Object getFieldValue(T o) {
    return mAbstracter.getFieldValue(o);
  }

  @Override
  public void put(T object) {
    Object fieldValue = getFieldValue(object);

    if (mIndexMap.putIfAbsent(fieldValue, object) != null) {
      throw new IllegalStateException("Adding more than one value to a unique index.");
    }
  }

  @Override
  public boolean remove(T object) {
    Object fieldValue = getFieldValue(object);
    return mIndexMap.remove(fieldValue, object);
  }

  @Override
  public boolean contains(Object value) {
    return mIndexMap.containsKey(value);
  }

  @Override
  public Set<T> getByField(Object value) {
    Set<T> set = new HashSet<T>();
    T res = mIndexMap.get(value);
    if (res != null) {
      set.add(res);
    }
    return Collections.unmodifiableSet(set);
  }

  @Override
  public T getFirst(Object value) {
    return mIndexMap.get(value);
  }
}
