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
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class representing a unique index. A unique index is an index
 * where each index value only maps to one object.
 *
 * @param <T> type of objects in this index
 * @param <V> type of the field used for indexing
 */
@ThreadSafe
public class UniqueFieldIndex<T, V> implements FieldIndex<T, V> {
  private final IndexDefinition<T, V> mIndexDefinition;
  private final ConcurrentHashMap<V, T> mIndexMap;

  /**
   * Constructs a new {@link UniqueFieldIndex} instance.
   *
   * @param indexDefinition definition of index
   */
  public UniqueFieldIndex(IndexDefinition<T, V> indexDefinition) {
    mIndexMap = new ConcurrentHashMap<>(8, 0.95f, 8);
    mIndexDefinition = indexDefinition;
  }

  @Override
  public boolean add(T object) {
    V fieldValue = mIndexDefinition.getFieldValue(object);
    T previousObject = mIndexMap.putIfAbsent(fieldValue, object);

    if (previousObject != null && previousObject != object) {
      return false;
    }
    return true;
  }

  @Override
  public boolean remove(T object) {
    V fieldValue = mIndexDefinition.getFieldValue(object);
    return mIndexMap.remove(fieldValue, object);
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
    T res = mIndexMap.get(fieldValue);
    if (res == null) {
      return false;
    }
    return res == object;
  }

  @Override
  public Set<T> getByField(V value) {
    T res = mIndexMap.get(value);
    if (res != null) {
      return Collections.singleton(res);
    }
    return Collections.emptySet();
  }

  @Override
  public T getFirst(V value) {
    return mIndexMap.get(value);
  }

  @Override
  public Iterator<T> iterator() {
    return mIndexMap.values().iterator();
  }

  @Override
  public int size() {
    return mIndexMap.size();
  }
}
