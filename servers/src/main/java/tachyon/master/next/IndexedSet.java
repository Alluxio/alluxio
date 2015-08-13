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

import com.google.common.base.Throwables;

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
  private final Map<String, Integer> mIndexMap;
  /** List of maps from fieldValue to set of T which is indexed by field name */
  private final List<Map<Object, Set<T>>> mSetIndexedByFieldValue;
  /** List of {@link Field} which is indexed by field name */
  private final List<Field> mFields;

  /**
   * A wrapper around a field name string to force the user to predefine the indexes, the user can
   * reuse the instance of this class as parameters later other than directly use the error prone
   * raw Strings.
   */
  public static class FieldIndex {
    private String mFieldName;

    public FieldIndex(String fieldName) {
      mFieldName = fieldName;
    }

    public String getFieldName() {
      return mFieldName;
    }
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
  public IndexedSet(FieldIndex field, FieldIndex... otherFields) {
    mIndexMap = new HashMap<String, Integer>(otherFields.length + 1);
    mIndexMap.put(field.getFieldName(), 0);
    for (int i = 1; i <= otherFields.length; i ++) {
      mIndexMap.put(otherFields[i - 1].getFieldName(), i);
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
   */
  public void add(T object) {
    synchronized (mFields) {
      for (String field : mIndexMap.keySet()) {
        Map<Object, Set<T>> fieldValueToSet = mSetIndexedByFieldValue.get(mIndexMap.get(field));
        Object value = getField(object, field);
        if (fieldValueToSet.containsKey(value)) {
          fieldValueToSet.get(value).add(object);
        } else {
          Set<T> set = new HashSet<T>();
          set.add(object);
          fieldValueToSet.put(value, set);
        }
      }
    }
  }

  /**
   * @return a set of all objects
   */
  public Set<T> all() {
    Map<Object, Set<T>> setForOneField;
    synchronized (mFields) {
      setForOneField = mSetIndexedByFieldValue.get(0);
    }
    Set<T> ret = new HashSet<T>();
    for (Set<T> set : setForOneField.values()) {
      ret.addAll(set);
    }
    return ret;
  }

  /**
   * Whether there is an object with the specified field value in the set.
   *
   * @param index the field index
   * @param value the field value
   * @return true if there is one such object, otherwise false
   */
  public boolean contains(FieldIndex index, Object value) {
    return !getByField(index, value).isEmpty();
  }

  /**
   * Get a subset of objects with the specified field value.
   *
   * @param index the field index
   * @param value the field value to be satisfied
   * @return the set of objects or an empty set if no such object exists
   */
  public Set<T> getByField(FieldIndex index, Object value) {
    int id = getIndex(index);
    Set<T> set;
    synchronized (mFields) {
      set = mSetIndexedByFieldValue.get(id).get(value);
    }
    return set == null ? new HashSet<T>() : set;
  }

  /**
   * Get the first object from the set of objects with the specified field value.
   *
   * @param index the field index
   * @param value the field value
   * @return the object or null if there is no such object
   */
  public T getFirst(FieldIndex index, Object value) {
    Set<T> all = getByField(index, value);
    return all.isEmpty() ? null : all.iterator().next();
  }

  /**
   * Remove an object from the set.
   *
   * @param object the object to remove
   * @return true if removed successfully, otherwise false
   */
  public boolean remove(T object) {
    boolean success = true;
    for (String field : mIndexMap.keySet()) {
      Object fieldValue = getField(object, field);
      int id = mIndexMap.get(field);
      synchronized (mFields) {
        Set<T> set = mSetIndexedByFieldValue.get(id).remove(fieldValue);
        if (set != null) {
          success = success && set.remove(object);
          if (!set.isEmpty()) {
            mSetIndexedByFieldValue.get(id).put(fieldValue, set);
          }
        }
      }
    }
    return success;
  }

  /**
   * Remove the subset of objects with the specified field value.
   *
   * @param index the field index
   * @param value the field value
   * @return true if the objects are removed, otherwise false
   */
  public boolean removeByField(FieldIndex index, Object value) {
    boolean success = true;
    Set<T> toRemove = new HashSet<T>();
    synchronized (mFields) {
      Set<T> remove = mSetIndexedByFieldValue.get(getIndex(index)).get(value);
      if (remove != null) {
        toRemove.addAll(remove);
      }
    }
    for (T o : toRemove) {
      success = success && remove(o);
    }
    return success;
  }

  /**
   * @return number of all objects
   */
  public int size() {
    return all().size();
  }

  /**
   * Get the value of the field from the object.
   *
   * @param object the object
   * @param field the field name
   * @return the field value
   * @throws RuntimeException when object doesn't have field
   */
  private Object getField(T object, String field) {
    try {
      synchronized (mFields) {
        Field f = mFields.get(mIndexMap.get(field));
        if (f == null) {
          f = object.getClass().getDeclaredField(field);
          f.setAccessible(true);
          mFields.set(mIndexMap.get(field), f);
        }
        return f.get(object);
      }
    } catch (Exception e) {
      throw Throwables.propagate(e); // No exception should happen
    }
  }

  private int getIndex(FieldIndex index) {
    return mIndexMap.get(index.getFieldName());
  }
}
