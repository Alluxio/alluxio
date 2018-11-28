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

import alluxio.Configuration;
import alluxio.PropertyKey;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class representing a composite unique index. A unique index is an index
 * where each index value only maps to one object.
 *
 * @param <T> type of objects in this index
 * @param <V> type of the field used for indexing
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
    value = "BC_UNCONFIRMED_CAST",
    justification = "Casts are checked, but findbugs doesn't recognize")
@ThreadSafe
public final class CompositeUniqueFieldIndex<T extends Comparable<? super V>, V>
    implements FieldIndex<T, V> {
  // Profiling shows that most of the file lists are between 1 and 4 elements.
  // Thus allocate the corresponding ArrayLists with a small initial capacity.
  private static final int DEFAULT_FILES_PER_DIRECTORY = 2;
  // Use map to store objects when the number of objects exceeds
  // MAP_THRESHOLD, otherwise use list to store objects.
  private static final int MAP_THRESHOLD =
      Configuration.getInt(PropertyKey.MASTER_METE_DATE_INODE_DIRECTORY_MAP_THRESHOLD);
  private final IndexDefinition<T, V> mIndexDefinition;
  private transient Object mChildren;

  /**
   * Constructs a new {@link CompositeUniqueFieldIndex} instance.
   *
   * @param indexDefinition definition of index
   */
  public CompositeUniqueFieldIndex(IndexDefinition<T, V> indexDefinition) {
    mIndexDefinition = indexDefinition;
  }

  @Override
  public synchronized boolean add(T object) {
    if (!(mChildren instanceof Map) && size() > MAP_THRESHOLD) {
      list2Map();
    }
    if (mChildren instanceof Map) {
      return addMap(object, (Map<V, T>) mChildren);
    }
    return addList(object, (List<T>) mChildren);
  }

  private boolean addList(T object, List<T> mObjectList) {
    int low = searchObject(object, mObjectList);
    if (low >= 0) {
      return false;
    }
    addChild(object, low);
    return true;
  }

  private boolean addMap(T object, Map<V, T> mIndexMap) {
    V fieldValue = mIndexDefinition.getFieldValue(object);
    T previousObject = mIndexMap.putIfAbsent(fieldValue, object);

    if (previousObject != null && previousObject != object) {
      return false;
    }
    return true;
  }

  private void list2Map() {
    List<T> mObjectList = (List<T>) mChildren;
    Map<V, T> mIndexMap = new ConcurrentHashMap<>(mObjectList.size() + 1, 0.95f, 8);
    for (T object : mObjectList) {
      addMap(object, mIndexMap);
    }
    mChildren = mIndexMap;
  }

  /**
   * Add the node to the mObjectList list at the given insertion point.
   * The basic add method which actually calls mObjectList.add(..).
   */
  private void addChild(final T object, final int insertionPoint) {
    if (mChildren == null) {
      mChildren = new CopyOnWriteArrayList<>(new ArrayList<>(DEFAULT_FILES_PER_DIRECTORY));
    }
    List<T> mObjectList = (List<T>) mChildren;
    mObjectList.add(-insertionPoint - 1, object);
  }

  @Override
  public synchronized boolean remove(T object) {
    if (mChildren instanceof Map && size() < MAP_THRESHOLD) {
      map2List();
    }
    if (mChildren instanceof Map) {
      return removeMap(object, (Map<V, T>) mChildren);
    }
    return removeList(object, (List<T>) mChildren);
  }

  private boolean removeList(T object, List<T> mObjectList) {
    int i = searchObject(object, mObjectList);
    if (i < 0) {
      return false;
    }
    T removed = mObjectList.remove(i);
    Preconditions.checkState(removed == object);
    return true;
  }

  private boolean removeMap(T object, Map<V, T> mIndexMap) {
    V fieldValue = mIndexDefinition.getFieldValue(object);
    return mIndexMap.remove(fieldValue, object);
  }

  private void map2List() {
    Map<V, T> mIndexMap = (Map<V, T>) mChildren;
    int len = DEFAULT_FILES_PER_DIRECTORY;
    if (mIndexMap.size() > DEFAULT_FILES_PER_DIRECTORY) {
      len = mIndexMap.size();
    }
    List<T> mObjectList = new CopyOnWriteArrayList<>(new ArrayList<>(len));

    for (T object : mIndexMap.values()) {
      addList(object, mObjectList);
    }
    mChildren = mObjectList;
  }

  @Override
  public synchronized void clear() {
    mChildren = null;
  }

  @Override
  public synchronized boolean containsField(V fieldValue) {
    if (mChildren instanceof Map) {
      return containsFieldMap(fieldValue, (Map<V, T>) mChildren);
    } else {
      return containsFieldList(fieldValue, (List<T>) mChildren);
    }
  }

  private boolean containsFieldMap(V fieldValue, Map<V, T> mIndexMap) {
    return mIndexMap.containsKey(fieldValue);
  }

  private boolean containsFieldList(V fieldValue, List<T> mObjectList) {
    final int i = searchField(fieldValue, mObjectList);
    return i > -1;
  }

  @Override
  public synchronized boolean containsObject(T object) {
    if (mChildren instanceof Map) {
      return containsObjectMap(object, (Map<V, T>) mChildren);
    }
    return containsObjectList(object, (List<T>) mChildren);
  }

  private boolean containsObjectMap(T object, Map<V, T> mIndexMap) {
    V fieldValue = mIndexDefinition.getFieldValue(object);
    T res = mIndexMap.get(fieldValue);
    if (res == null) {
      return false;
    }
    return res == object;
  }

  private boolean containsObjectList(T object, List<T> mObjectList) {
    final int i = searchObject(object, mObjectList);
    return i >= 0 && mObjectList.get(i) == object;
  }

  @Override
  public synchronized Set<T> getByField(V value) {
    if (mChildren instanceof Map) {
      return getByFieldMap(value, (Map<V, T>) mChildren);
    }
    return getByFieldList(value, (List<T>) mChildren);
  }

  private Set<T> getByFieldMap(V value, Map<V, T> mIndexMap) {
    T res = mIndexMap.get(value);
    if (res != null) {
      return Collections.singleton(res);
    }
    return Collections.emptySet();
  }

  private Set<T> getByFieldList(V value, List<T> mObjectList) {
    final int i = searchField(value, mObjectList);
    if (i < 0) {
      return Collections.emptySet();
    }
    return Collections.singleton(mObjectList.get(i));
  }

  @Override
  public synchronized T getFirst(V value) {
    if (mChildren instanceof Map) {
      return getFirstMap(value, (Map<V, T>) mChildren);
    }
    return getFirstList(value, (List<T>) mChildren);
  }

  private T getFirstMap(V value, Map<V, T> mIndexMap) {
    return mIndexMap.get(value);
  }

  private T getFirstList(V value, List<T> mObjectList) {
    final int i = searchField(value, mObjectList);
    if (i < 0) {
      return null;
    }
    return mObjectList.get(i);
  }

  @Override
  public synchronized Iterator<T> iterator() {
    if (mChildren instanceof Map) {
      return ((Map<V, T>) mChildren).values().iterator();
    } else if (mChildren != null) {
      return ((List<T>) mChildren).iterator();
    } else {
      return Collections.emptyIterator();
    }
  }

  /**
   * Returns an unmodifiable collection over the elements in this index.
   * The elements are returned in no particular order.
   * Note that the behavior of the iterator is unspecified if the underlying collection is modified
   * while a thread is going through the collection.
   *
   * @return an unmodifiable view of the specified collection {@link FieldIndex}
   */
  public synchronized Collection<T> readOnlyValues() {
    if (mChildren instanceof Map) {
      return Collections.unmodifiableCollection(((Map<V, T>) mChildren).values());
    } else if (mChildren != null) {
      return Collections.unmodifiableCollection((List<T>) mChildren);
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public synchronized int size() {
    if (mChildren instanceof Map) {
      return ((Map<V, T>) mChildren).size();
    }
    return mChildren == null ? 0 : ((List<T>) mChildren).size();
  }

  private int searchObject(T object, List<T> mObjectList) {
    V fieldValue = mIndexDefinition.getFieldValue(object);
    return searchField(fieldValue, mObjectList);
  }

  private int searchField(V fieldValue, List<T> mObjectList) {
    return mObjectList == null ? -1 : Collections.binarySearch(mObjectList, fieldValue);
  }
}
