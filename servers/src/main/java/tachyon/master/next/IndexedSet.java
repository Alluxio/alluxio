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

package tachyon.master.next;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.lang.reflect.Field;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * A set of objects that can be indexed by specific fields of the object, and different IndexedSet
 * instances can use different fields to index. The field type must be comparable. The field value
 * must not be changed after being added to the set, otherwise, behavior for all operations is not
 * specified.
 *
 * In operations that need field name as the parameter, if the object does not have the field, a
 * RuntimeException will be thrown.
 *
 * This class is thread safe.
 */
public class IndexedSet<T> {
  /** Map from field index to an index of field related object in the internal lists */
  private final Map<FieldIndex<T>, Integer> mIndexMap;
  /** List of maps from fieldValue to set of T which is indexed by field name */
  private final List<Map<Object, Set<T>>> mSetIndexedByFieldValue;
  /** List of {@link Field} which is indexed by field name */
  private final List<Field> mFields;
  /** Number of all objects */
  private int mSize = 0;

  /**
   * A wrapper around a field name string to force the user to predefine the indexes, the user can
   * reuse the instance of this class as parameters later other than directly use the error prone
   * raw Strings.
   */
  public static interface FieldIndex<T> {
    Object getFieldValue(T o);
  }

  /**
   * Construct a new IndexedSet with at least one field as the index, the field can be either public
   * or private. The {@link Field}s for these fields aren't retrieved until {@link #add(Object)} is
   * called for the first time, it will be cached once it is retrieved so that reflection will only
   * be needed once for a field.
   *
   * @param field at least one field is needed to index the set of objects
   * @param otherFields other fields to index the set
   */
  // TODO: if reflections on T can be gotten in the constructor, validate the passed fields
  public IndexedSet(FieldIndex<T> field, FieldIndex<T>... otherFields) {
    mIndexMap = new HashMap<FieldIndex<T>, Integer>(otherFields.length + 1);
    mIndexMap.put(field, 0);
    for (int i = 1; i <= otherFields.length; i ++) {
      mIndexMap.put(otherFields[i - 1], i);
    }

    mFields = new ArrayList<Field>(mIndexMap.size());
    for (int i = 0; i < mIndexMap.size(); i ++) {
      mFields.add(null);
    }

    mSetIndexedByFieldValue = new ArrayList<Map<Object, Set<T>>>(mIndexMap.size());
    for (int i = 0; i < mIndexMap.size(); i ++) {
      mSetIndexedByFieldValue.add(new HashMap<Object, Set<T>>());
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
    synchronized (mFields) {
      boolean success = true;
      for (FieldIndex<T> field : mIndexMap.keySet()) {
        Map<Object, Set<T>> fieldValueToSet = mSetIndexedByFieldValue.get(mIndexMap.get(field));
        Object value = field.getFieldValue(object);
        if (fieldValueToSet.containsKey(value)) {
          if (!fieldValueToSet.get(value).add(object)) {
            success = false;
          }
        } else {
          fieldValueToSet.put(value, Sets.newHashSet(object));
        }
      }
      if (success) {
        mSize += 1;
      }
      return success;
    }
  }

  /**
   * Return the set of all objects, this method could be expensive, O(n) in worst case, n is the
   * number of all objects.
   *
   * @return a set of all objects
   */
  public Set<T> all() {
    synchronized (mFields) {
      Map<Object, Set<T>> setForOneField = mSetIndexedByFieldValue.get(0);
      Set<T> ret = new HashSet<T>();
      for (Set<T> set : setForOneField.values()) {
        ret.addAll(set);
      }
      return ret;
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
    synchronized (mFields) {
      return getByFieldInternal(index, value) != null;
    }
  }

  /**
   * Get a subset of objects with the specified field value. O(n) in worst case due to the copy of
   * the internal set.
   *
   * @param index the field index
   * @param value the field value to be satisfied
   * @return the set of objects or an empty set if no such object exists
   */
  public Set<T> getByField(FieldIndex<T> index, Object value) {
    synchronized (mFields) {
      Set<T> set = getByFieldInternal(index, value);
      return set == null ? new HashSet<T>() : Sets.newHashSet(set);
    }
  }


  /**
   * Get the first object from the set of objects with the specified field value.
   *
   * @param index the field index
   * @param value the field value
   * @return the object or null if there is no such object
   */
  public T getFirstByField(FieldIndex<T> index, Object value) {
    synchronized (mFields) {
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
    boolean removed = true;
    boolean triedToRemove = false;
    for (FieldIndex<T> field : mIndexMap.keySet()) {
      Object fieldValue = field.getFieldValue(object);

      int id = mIndexMap.get(field);
      synchronized (mFields) {
        Set<T> set = mSetIndexedByFieldValue.get(id).get(fieldValue);
        if (set != null) {
          triedToRemove = true;
          if (!set.remove(object)) {
            removed = false;
          }
          if (set.isEmpty()) {
            mSetIndexedByFieldValue.get(id).remove(fieldValue);
          }
        }
      }
    }
    boolean success = removed && triedToRemove;
    if (success) {
      synchronized (mFields) {
        mSize -= 1;
      }
    }
    return success;
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
    synchronized (mFields) {
      Set<T> toRemove = getByFieldInternal(index, value);
      if (toRemove == null) {
        return false;
      }
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
    return mSize;
  }

  private Set<T> getByFieldInternal(FieldIndex<T> index, Object value) {
    return mSetIndexedByFieldValue.get(mIndexMap.get(index)).get(value);
  }
}
