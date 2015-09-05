/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import tachyon.Constants;

/**
 * A set of objects that can be indexed by specific fields of the object, and different IndexedSet
 * instances can use different fields to index. The field type must be comparable. The field value
 * must not be changed after being added to the set, otherwise, behavior for all operations is not
 * specified.
 *
 * This class is thread safe.
 */
public class IndexedSet<T> implements Iterable<T> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** All objects in the set */
  private final Set<T> mObjects = new HashSet<T>();
  /** Map from field index to an index of field related object in the internal lists */
  private final Map<FieldIndex<T>, Integer> mIndexMap;
  /** List of maps from value of a specific field to set of objects with the field value */
  private final List<Map<Object, Set<T>>> mSetIndexedByFieldValue;
  /** Final object for synchronization */
  private final Object mLock = new Object();

  /**
   * An interface representing an index for this IndexedSet, each index for this set must implement
   * the interface to define how to get the value of the field chosen as the index. Users must use
   * the same instance of the implementation of this interface as the parameter in all methods of
   * IndexedSet to represent the same index.
   *
   * @param <T> type of objects in this IndexedSet
   */
  public static interface FieldIndex<T> {
    /**
     * Get the value of the field that serves as index.
     *
     * @param o the instance to get the field value from
     * @return the field value, which is just an Object
     */
    Object getFieldValue(T o);
  }

  /**
   * Construct a new IndexedSet with at least one field as the index.
   *
   * @param field at least one field is needed to index the set of objects
   * @param otherFields other fields to index the set
   */
  public IndexedSet(FieldIndex<T> field, FieldIndex<T>... otherFields) {
    mIndexMap = new HashMap<FieldIndex<T>, Integer>(otherFields.length + 1);
    mIndexMap.put(field, 0);
    for (int i = 1; i <= otherFields.length; i ++) {
      mIndexMap.put(otherFields[i - 1], i);
    }

    mSetIndexedByFieldValue = new ArrayList<Map<Object, Set<T>>>(mIndexMap.size());
    for (int i = 0; i < mIndexMap.size(); i ++) {
      mSetIndexedByFieldValue.add(new HashMap<Object, Set<T>>());
    }
  }

  /**
   * Removes all the entries in this set.
   */
  public void clear() {
    synchronized (mLock) {
      mObjects.clear();
      for (Map<Object, Set<T>> mapping : mSetIndexedByFieldValue) {
        mapping.clear();
      }
    }
  }

  /**
   * Add an object o to the set if there is no other object o2 such that (o == null ? o2 == null :
   * o.equals(o2)). If this set already contains the object, the call leaves the set unchanged.
   *
   * @param object the object to add
   * @return true if the object is successfully added to all indexes, otherwise false
   */
  public boolean add(T object) {
    synchronized (mLock) {
      boolean success = mObjects.add(object);
      for (Map.Entry<FieldIndex<T>, Integer> index : mIndexMap.entrySet()) {
        Map<Object, Set<T>> fieldValueToSet = mSetIndexedByFieldValue.get(index.getValue());
        Object value = index.getKey().getFieldValue(object);
        if (fieldValueToSet.containsKey(value)) {
          success = success && fieldValueToSet.get(value).add(object);
        } else {
          fieldValueToSet.put(value, Sets.newHashSet(object));
        }
      }
      return success;
    }
  }

  /**
   * Returns an iterator over the elements in this set. The elements are returned in no particular
   * order. It is to implement {@link Iterable} so that users can foreach the IndexedSet directly.
   * The traversal may reflect any modifications subsequent to the construction of the iterator.
   *
   * @return an iterator over the elements in this IndexedSet
   */
  @Override
  public Iterator<T> iterator() {
    synchronized (mLock) {
      return mObjects.iterator();
    }
  }

  /**
   * Whether there is an object with the specified field value in the set.
   *
   * @param index the field index
   * @param value the field value
   * @return true if there is one such object, otherwise false
   */
  public boolean contains(FieldIndex<T> index, Object value) {
    synchronized (mLock) {
      return getByFieldInternal(index, value) != null;
    }
  }

  /**
   * Gets a subset of objects with the specified field value. If there is no object with the
   * specified field value, a newly created empty set is returned. Otherwise, the returned set is
   * backed up by an internal set, so changes in internal set will be reflected in returned set and
   * vice-versa.
   *
   * @param index the field index
   * @param value the field value to be satisfied
   * @return the set of objects or an empty set if no such object exists
   */
  public Set<T> getByField(FieldIndex<T> index, Object value) {
    synchronized (mLock) {
      Set<T> set = getByFieldInternal(index, value);
      return set == null ? new HashSet<T>() : set;
    }
  }

  /**
   * Gets the first object from the set of objects with the specified field value.
   *
   * @param index the field index
   * @param value the field value
   * @return the object or null if there is no such object
   */
  public T getFirstByField(FieldIndex<T> index, Object value) {
    synchronized (mLock) {
      Set<T> all = getByFieldInternal(index, value);
      return all == null ? null : all.iterator().next();
    }
  }

  /**
   * Remove an object from the set.
   *
   * @param object the object to remove
   * @return true if the object is in the set and removed successfully, otherwise false
   */
  public boolean remove(T object) {
    synchronized (mLock) {
      boolean success = mObjects.remove(object);
      for (Map.Entry<FieldIndex<T>, Integer> index : mIndexMap.entrySet()) {
        Object fieldValue = index.getKey().getFieldValue(object);
        Map<Object, Set<T>> fieldValueToSet = mSetIndexedByFieldValue.get(index.getValue());
        Set<T> set = fieldValueToSet.get(fieldValue);
        if (set != null) {
          if (!set.remove(object)) {
            LOG.error("Fail to remove object " + object.toString() + " from IndexedSet.");
            success = false;
          }
          if (set.isEmpty()) {
            fieldValueToSet.remove(fieldValue);
          }
        }
      }
      return success;
    }
  }

  /**
   * Remove the subset of objects with the specified field value.
   *
   * @param index the field index
   * @param value the field value
   * @return true if the objects are removed, otherwise if some objects fail to be removed or no
   *         objects are removed, return false
   */
  public boolean removeByField(FieldIndex<T> index, Object value) {
    synchronized (mLock) {
      Set<T> toRemove = getByFieldInternal(index, value);
      if (toRemove == null) {
        return false;
      }
      // Copy the set so that no ConcurrentModificationException happens
      toRemove = ImmutableSet.copyOf(toRemove);
      boolean success = true;
      for (T o : toRemove) {
        success = success && remove(o);
      }
      return success;
    }
  }

  /**
   * @return number of all objects, O(1)
   */
  public int size() {
    synchronized (mLock) {
      return mObjects.size();
    }
  }

  private Set<T> getByFieldInternal(FieldIndex<T> index, Object value) {
    return mSetIndexedByFieldValue.get(mIndexMap.get(index)).get(value);
  }
}
