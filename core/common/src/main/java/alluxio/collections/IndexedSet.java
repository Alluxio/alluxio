/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.collections;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A set of objects that are indexed and thus can be queried by specific fields of the object.
 * Different {@link IndexedSet} instances may specify different fields to index. The field type
 * must be comparable. The field value must not be changed after an object is added to the set,
 * otherwise, behavior for all operations is not specified.
 *
 * <p>
 * Example usage:
 *
 * We have a set of puppies:
 * <pre>
 *   class Puppy {
 *     private final String mName;
 *     private final long mId;
 *
 *     public Puppy(String name, long id) {
 *       mName = name;
 *       mId = id;
 *     }
 *
 *     public String name() {
 *       return mName;
 *     }
 *
 *     public long id() {
 *       return mId;
 *     }
 *   }
 * </pre>
 *
 * We want to be able to retrieve the set of puppies via a puppy's id or name, one way is to have
 * two maps like {@code Map<String, Puppy> nameToPuppy} and {@code Map<Long, Puppy> idToPuppy},
 * another way is to use a single instance of {@link IndexedSet}!
 *
 * First, define the fields to be indexed:
 * <pre>
 *  FieldIndex<Puppy> idIndex = new FieldIndex<Puppy> {
 *    {@literal @Override}
 *    Object getFieldValue(Puppy o) {
 *      return o.id();
 *    }
 *  }
 *
 *  FieldIndex<Puppy> nameIndex = new FieldIndex<Puppy> {
 *    {@literal @Override}
 *    Object getFieldValue(Puppy o) {
 *      return o.name();
 *    }
 *  }
 * </pre>
 *
 * Then create an {@link IndexedSet} and add puppies:
 * <pre>
 *  IndexedSet<Puppy> puppies = new IndexedSet<Puppy>(idIndex, nameIndex);
 *  puppies.add(new Puppy("sweet", 0));
 *  puppies.add(new Puppy("heart", 1));
 * </pre>
 *
 * Then retrieve the puppy named sweet:
 * <pre>
 *   Puppy sweet = puppies.getFirstByField(nameIndex, "sweet");
 * </pre>
 * and retrieve the puppy with id 1:
 * <pre>
 *   Puppy heart = puppies.getFirstByField(idIndex, 1L);
 * </pre>
 *
 * @param <T> the type of object
 */
@ThreadSafe
public class IndexedSet<T> implements Iterable<T> {
  /** All objects in the set. */
  private final Set<T> mObjects = new HashSet<T>();
  /** Map from field index to an index of field related object in the internal lists. */
  private final Map<FieldIndex<T>, Integer> mIndexMap;
  /** List of maps from value of a specific field to set of objects with the field value. */
  private final List<Map<Object, Set<T>>> mSetIndexedByFieldValue;
  /** Final object for synchronization. */
  private final Object mLock = new Object();

  /**
   * An interface representing an index for this {@link IndexedSet}, each index for this set must
   * implement the interface to define how to get the value of the field chosen as the index. Users
   * must use the same instance of the implementation of this interface as the parameter in all
   * methods of {@link IndexedSet} to represent the same index.
   *
   * @param <T> type of objects in this {@link IndexedSet}
   */
  public interface FieldIndex<T> {
    /**
     * Gets the value of the field that serves as index.
     *
     * @param o the instance to get the field value from
     * @return the field value, which is just an Object
     */
    Object getFieldValue(T o);
  }

  /**
   * Constructs a new {@link IndexedSet} instance with at least one field as the index.
   *
   * @param field at least one field is needed to index the set of objects
   * @param otherFields other fields to index the set
   */
  public IndexedSet(FieldIndex<T> field, FieldIndex<T>... otherFields) {
    mIndexMap = new HashMap<FieldIndex<T>, Integer>(otherFields.length + 1);
    mIndexMap.put(field, 0);
    for (int i = 1; i <= otherFields.length; i++) {
      mIndexMap.put(otherFields[i - 1], i);
    }

    mSetIndexedByFieldValue = new ArrayList<Map<Object, Set<T>>>(mIndexMap.size());
    for (int i = 0; i < mIndexMap.size(); i++) {
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
   * Adds an object o to the set if there is no other object o2 such that
   * {@code (o == null ? o2 == null : o.equals(o2))}. If this set already contains the object, the
   * call leaves the set unchanged.
   *
   * @param objToAdd the object to add
   * @return true if the object is successfully added to all indexes, otherwise false
   */
  public boolean add(T objToAdd) {
    Preconditions.checkNotNull(objToAdd);
    synchronized (mLock) {
      boolean success = mObjects.add(objToAdd);
      // iterate over the first level index (filedid to map)
      if (success) {
        for (Map.Entry<FieldIndex<T>, Integer> index : mIndexMap.entrySet()) {
          // get the second level map
          Map<Object, Set<T>> fieldValueToSet = mSetIndexedByFieldValue.get(index.getValue());
          // retrieve the key to index the object within the second level map
          Object fieldValue = index.getKey().getFieldValue(objToAdd);
          if (fieldValueToSet.containsKey(fieldValue)) {
            success = fieldValueToSet.get(fieldValue).add(objToAdd);
            if (!success) {
              // this call can never return false because:
              //   a. the second-level sets in the indices are all
              //      {@link kava.util.HashSet} instances of unbounded space
              //   b. We have already successfully added objToAdd on mObjects,
              //      meaning that it cannot be already in any of the sets.
              //      (mObjects is exactly the set-union of all the other second-level sets)
              throw new IllegalStateException("Indexed Set is in an illegal state");
            }
          } else {
            fieldValueToSet.put(fieldValue, Sets.newHashSet(Collections.singleton(objToAdd)));
          }
        }
      }

      return success;
    }
  }

  /**
   * Returns an iterator over the elements in this set. The elements are returned in no particular
   * order. It is to implement {@link Iterable} so that users can foreach the {@link IndexedSet}
   * directly.
   *
   * Note that the behaviour of the iterator is unspecified if the underlying collection is
   * modified while a thread is going through the iterator.
   *
   * @return an iterator over the elements in this {@link IndexedSet}
   */
  @Override
  public Iterator<T> iterator() {
    return new IndexedSetIterator();
  }

  /**
   *  Specialized iterator for {@link IndexedSet}.
   *
   *  This is needed to support consistent removal from the set and the indices.
   */
  private class IndexedSetIterator implements Iterator<T> {
    private final Iterator<T> mSetIterator;
    private T mLast;

    public IndexedSetIterator() {
      mSetIterator = mObjects.iterator();
      mLast = null;
    }

    @Override
    public boolean hasNext() {
      return mSetIterator.hasNext();
    }

    @Override
    public T next() {
      final T next = mSetIterator.next();
      mLast = next;
      return next;
    }

    @Override
    public void remove() {
      if (mLast != null) {
        synchronized (mLock) {
          mSetIterator.remove();
          removeFromIndices(mLast);
          mLast = null;
        }
      } else {
        throw new IllegalStateException("Next was never called before");
      }
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
   * Removes an object from the set.
   *
   * @param object the object to remove
   * @return true if the object is in the set and removed successfully, otherwise false
   */
  public boolean remove(T object) {
    synchronized (mLock) {
      boolean success = mObjects.remove(object);
      if (success) {
        removeFromIndices(object);
      }
      return success;
    }
  }

  /**
   * Helper method that removes an object from the two level-indices structure.
   * Precondition: {@code object} was in {@link #mObjects} and was just successfully removed from
   * it before calling this method.
   *
   * Refactored for being called from both {@link #remove(Object)} and {@link alluxio.collections
   * .IndexedSet.IndexedSetIterator#remove()}
   *
   * @param object the object to be removed
   */
  private void removeFromIndices(T object) {
    for (Map.Entry<FieldIndex<T>, Integer> index : mIndexMap.entrySet()) {
      Object fieldValue = index.getKey().getFieldValue(object);
      Map<Object, Set<T>> fieldValueToSet = mSetIndexedByFieldValue.get(index.getValue());
      Set<T> set = fieldValueToSet.get(fieldValue);
      if (set != null) {
        if (!set.remove(object)) {
          //runtime exception rather than returning false,
          //since it would mean that the IndexedSet is in an inconsistent state --> BUG
          throw new IllegalStateException("Fail to remove object " + object.toString()
              + "from IndexedSet.");
        }
        if (set.isEmpty()) {
          fieldValueToSet.remove(fieldValue);
        }
      }
    }
  }

  /**
   * Removes the subset of objects with the specified field value.
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
   * @return the number of objects in this indexed set (O(1) time)
   */
  public int size() {
    synchronized (mLock) {
      return mObjects.size();
    }
  }

  /**
   * Gets the set of objects with the specified field value - internal function.
   *
   * @param index the field index
   * @param value the field value
   * @return the set of objects with the specified field value
   */
  private Set<T> getByFieldInternal(FieldIndex<T> index, Object value) {
    return mSetIndexedByFieldValue.get(mIndexMap.get(index)).get(value);
  }
}
