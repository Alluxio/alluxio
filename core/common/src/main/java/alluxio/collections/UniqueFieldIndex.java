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

import io.netty.util.internal.chmv8.ConcurrentHashMapV8;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class representing a unique index. A unique index is an index
 * where each index value only maps to one object.
 *
 * @param <T> type of objects in this index
 */
@ThreadSafe
public class UniqueFieldIndex<T> implements FieldIndex<T> {
  private final IndexDefinition<T> mIndexDefinition;
  private final ConcurrentHashMapV8<Object, T> mIndexMap;

  /**
   * Constructs a new {@link UniqueFieldIndex} instance.
   *
   * @param indexDefinition definition of index
   */
  public UniqueFieldIndex(IndexDefinition<T> indexDefinition) {
    mIndexMap = new ConcurrentHashMapV8<>(8, 0.95f, 8);
    mIndexDefinition = indexDefinition;
  }

  @Override
  public boolean add(T object) {
    Object fieldValue = mIndexDefinition.getFieldValue(object);
    T previousObject = mIndexMap.putIfAbsent(fieldValue, object);

    if (previousObject != null && previousObject != object) {
      return false;
    }
    return true;
  }

  @Override
  public boolean remove(T object) {
    Object fieldValue = mIndexDefinition.getFieldValue(object);
    return mIndexMap.remove(fieldValue, object);
  }

  @Override
  public void clear() {
    mIndexMap.clear();
  }

  @Override
  public boolean containsField(Object fieldValue) {
    return mIndexMap.containsKey(fieldValue);
  }

  @Override
  public boolean containsObject(T object) {
    Object fieldValue = mIndexDefinition.getFieldValue(object);
    T res = mIndexMap.get(fieldValue);
    if (res == null) {
      return false;
    }
    return res == object;
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

  @Override
  public Iterator<T> iterator() {
    return mIndexMap.values().iterator();
  }

  @Override
  public int size() {
    return mIndexMap.size();
  }
}
