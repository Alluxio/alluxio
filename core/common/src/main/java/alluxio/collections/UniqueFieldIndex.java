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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class representing a unique index. A unique index is an index
 * where each index value only maps to one object.
 *
 * @param <T> type of objects in this {@link IndexedSet}
 */
@ThreadSafe
class UniqueFieldIndex<T> implements FieldIndex<T> {
  private final IndexDefinition<T> mIndexDefinition;
  private final ConcurrentHashMap<Object, T> mIndexMap;

  /**
   * Constructs a new {@link UniqueFieldIndex} instance.
   */
  public UniqueFieldIndex(IndexDefinition<T> indexDefinition) {
    mIndexMap = new ConcurrentHashMap<>(8, 0.95f, 8);
    mIndexDefinition = indexDefinition;
  }

  @Override
  public void add(T object) {
    Object fieldValue = mIndexDefinition.getFieldValue(object);

    if (mIndexMap.putIfAbsent(fieldValue, object) != null) {
      throw new IllegalStateException("Adding more than one value to a unique index:"
          + fieldValue.toString());
    }
  }

  @Override
  public boolean remove(T object) {
    Object fieldValue = mIndexDefinition.getFieldValue(object);
    return mIndexMap.remove(fieldValue, object);
  }

  @Override
  public boolean contains(Object value) {
    return mIndexMap.containsKey(value);
  }

  @Override
  public Set<T> getByField(Object value) {
    T res = mIndexMap.get(value);
    if (res != null) {
      return Collections.singleton(res);
    }
    return Collections.emptySet();
  }

  @Override
  public T getFirst(Object value) {
    return mIndexMap.get(value);
  }
}
