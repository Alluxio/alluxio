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

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.lang.reflect.Field;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

/**
 * A set of objects that can be indexed by fields of the object, the field type must be comparable.
 * In operations that need field name as the parameter, if the object does not have the field, a
 * RuntimeException will be thrown.
 */
public class IndexedSet<T> {
  /** Map from fieldName to a map from fieldValue to set of T */
  private Map<String, Map<Object, Set<T>>> mSetIndexedByFieldValue;
  /** Map to cache the relation from fieldName to Field which is set accessible */
  private Map<String, Field> mFields;

  /**
   * Construct a new IndexSet.
   *
   * @param fields the field names to index the set of objects
   */
  public IndexedSet(String... fields) {
    mFields = Maps.newHashMap();
    for (String field : fields) {
      mFields.put(field, null);
    }

    mSetIndexedByFieldValue = Maps.newHashMap();
    for (String field : fields) {
      mSetIndexedByFieldValue.put(field, new HashMap<Object, Set<T>>());
    }
  }

  /**
   * Add an object to the set.
   *
   * @param object the object to add
   */
  public void add(T object) {
    for (String field : mFields.keySet()) {
      Map<Object, Set<T>> fieldValueToSet = mSetIndexedByFieldValue.get(field);
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

  /**
   * @return a set of all objects
   */
  public Set<T> all() {
    Map<Object, Set<T>> setForOneField = mSetIndexedByFieldValue.values().iterator().next();
    Set<T> ret = new HashSet<T>();
    for (Set<T> set : setForOneField.values()) {
      ret.addAll(set);
    }
    return ret;
  }

  /**
   * Whether there is an object with the specified field value in the set.
   *
   * @param fieldName the field name
   * @param value the field value
   * @return true if there is one such object, otherwise false
   */
  public boolean contains(String fieldName, Object value) {
    return !getByField(fieldName, value).isEmpty();
  }

  /**
   * Get a subset of objects with the specified field value.
   *
   * @param fieldName the field name
   * @param value the field value to be satisfied
   * @return the set of objects or an empty set if no such object exists
   */
  public Set<T> getByField(String fieldName, Object value) {
    Set<T> set = mSetIndexedByFieldValue.get(fieldName).get(value);
    return set == null ? new HashSet<T>() : set;
  }

  /**
   * Get the first object from the set of objects with the specified field value.
   *
   * @param fieldName the field name
   * @param value the field value
   * @return the object or null if there is no such object
   */
  public T getFirst(String fieldName, Object value) {
    Set<T> all = getByField(fieldName, value);
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
    for (String field : mFields.keySet()) {
      Object fieldValue = getField(object, field);
      Set<T> set = mSetIndexedByFieldValue.get(field).remove(fieldValue);
      if (set != null) {
        success = success && set.remove(object);
        if (!set.isEmpty()) {
          mSetIndexedByFieldValue.get(field).put(fieldValue, set);
        }
      }
    }
    return success;
  }

  /**
   * Remove the subset of objects with the specified field value.
   *
   * @param fieldName the field name
   * @param value the field value
   * @return true if the objects are removed, otherwise false
   */
  public boolean removeByField(String fieldName, Object value) {
    Set<T> toRemove = mSetIndexedByFieldValue.get(fieldName).remove(value);
    boolean success = true;
    if (toRemove != null) {
        for (Map<Object, Set<T>> index : mSetIndexedByFieldValue.values()) {
          for (Set<T> set : index.values()) {
            for (T obj : toRemove) {
            success = success && set.remove(obj);
          }
        }
      }
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
      Field f = mFields.get(field);
      if (f == null) {
        f = object.getClass().getDeclaredField(field);
        f.setAccessible(true);
        mFields.put(field, f);
      }
      return f.get(object);
    } catch (Exception e) {
      throw Throwables.propagate(e); // No exception should happen
    }
  }
}
