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

import java.util.Set;

/**
 * An interface representing an index for this {@link IndexedSet}, each index for this set must
 * implement the interface to define how to get the value of the field chosen as the index. Users
 * must use the same instance of the implementation of this interface as the parameter in all
 * methods of {@link IndexedSet} to represent the same index.
 *
 * @param <T> type of objects in this {@link IndexedSet}
 */
interface FieldIndex<T> {
  /**
   * Adds an object o to the index.
   *
   * @param o the object to add to the index
   */
  void add(T o);

  /**
   * Removes the object o from the index.
   *
   * @param o the object to remove from index
   * @return whether the specified element was in the index
   */
  boolean remove(T o);

  /**
   * Returns whether there is an object with the specified index field value in the set.
   *
   * @param value the field value to be satisfied
   * @return true if there is one such object, otherwise false
   */
  boolean contains(Object value);

  /**
   * Gets a subset of objects with the specified field value. If there is no object with
   * the specified field value, an empty set is returned.
   *
   * @param value the field value to be satisfied
   * @return the set of objects or an empty set if no such object exists
   */
  Set<T> getByField(Object value);

  /**
   * Gets an object from the set of objects with the specified field value.
   *
   * @param value the field value to be satisfied
   * @return the object or null if there is no such object
   */
  T getFirst(Object value);
}
