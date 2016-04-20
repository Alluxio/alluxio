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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
  /** All objects in the set. This set is required to guarantee uniqueness of objects. */
  // TODO(gpang): remove this set, and just use the indexes.
  private final ConcurrentHashSet<T> mObjects = new ConcurrentHashSet<>();
  /**
   * Map from {@link FieldIndex} to the index. An index is a map from index value to set of
   * objects with that index value.
   */
  private final Map<FieldIndex<T>, ConcurrentHashMap<Object, ConcurrentHashSet<T>>> mIndexMap;

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
    Map<FieldIndex<T>, ConcurrentHashMap<Object, ConcurrentHashSet<T>>> indexMap =
        new HashMap<>(otherFields.length + 1);
    indexMap.put(field, new ConcurrentHashMap<Object, ConcurrentHashSet<T>>());
    for (FieldIndex<T> fieldIndex : otherFields) {
      indexMap.put(fieldIndex, new ConcurrentHashMap<Object, ConcurrentHashSet<T>>());
    }
    // read only, so it is thread safe and allows concurrent access.
    mIndexMap = Collections.unmodifiableMap(indexMap);
  }

  /**
   * Removes all the entries in this set.
   *
   * This is an expensive operation, and concurrent adds are permitted.
   */
  public void clear() {
    for (T obj : mObjects) {
      remove(obj);
    }
  }

  /**
   * Adds an object o to the set if there is no other object o2 such that
   * {@code (o == null ? o2 == null : o.equals(o2))}. If this set already contains the object, the
   * call leaves the set unchanged.
   *
   * @param object the object to add
   * @return true if this set did not already contain the specified element
   */
  public boolean add(T object) {
    Preconditions.checkNotNull(object);

    synchronized (object) {
      if (!mObjects.addIfAbsent(object)) {
        // This object is already added, possibly by another concurrent thread.
        return false;
      }

      // Update the indexes.
      for (Map.Entry<FieldIndex<T>, ConcurrentHashMap<Object, ConcurrentHashSet<T>>> fieldInfo :
          mIndexMap.entrySet()) {
        // For this field, retrieve the value to index
        Object fieldValue = fieldInfo.getKey().getFieldValue(object);
        // Get the index for this field
        ConcurrentHashMap<Object, ConcurrentHashSet<T>> index = fieldInfo.getValue();
        ConcurrentHashSet<T> objSet = index.get(fieldValue);
        if (objSet == null) {
          index.putIfAbsent(fieldValue, new ConcurrentHashSet<T>());
          objSet = index.get(fieldValue);
        }
        if (!objSet.addIfAbsent(object)) {
          // this call can never return false because:
          //   a. the second-level sets in the indices are all
          //      {@link java.util.Set} instances of unbounded space
          //   b. We have already successfully added object on mObjects,
          //      meaning that it cannot be already in any of the sets.
          //      (mObjects is exactly the set-union of all the other second-level sets)
          throw new IllegalStateException("Indexed Set is in an illegal state");
        }
      }
    }
    return true;
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
    private T mObject;

    public IndexedSetIterator() {
      mSetIterator = mObjects.iterator();
      mObject = null;
    }

    @Override
    public boolean hasNext() {
      return mSetIterator.hasNext();
    }

    @Override
    public T next() {
      final T next = mSetIterator.next();
      mObject = next;
      return next;
    }

    @Override
    public void remove() {
      if (mObject != null) {
        IndexedSet.this.remove(mObject);
        mObject = null;
      } else {
        throw new IllegalStateException("next() was not called before remove()");
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
    ConcurrentHashSet<T> set = getByFieldInternal(index, value);
    return set != null && !set.isEmpty();
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
    Set<T> set = getByFieldInternal(index, value);
    return set == null ? new HashSet<T>() : set;
  }

  /**
   * Gets the first object from the set of objects with the specified field value.
   *
   * @param index the field index
   * @param value the field value
   * @return the object or null if there is no such object
   */
  public T getFirstByField(FieldIndex<T> index, Object value) {
    Set<T> all = getByFieldInternal(index, value);
    try {
      return all == null || !all.iterator().hasNext() ? null : all.iterator().next();
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  /**
   * Removes an object from the set.
   *
   * @param object the object to remove
   * @return true if the object is in the set and removed successfully, otherwise false
   */
  public boolean remove(T object) {
    synchronized (object) {
      if (mObjects.contains(object)) {
        removeFromIndices(object);
        return mObjects.remove(object);
      } else {
        return false;
      }
    }
  }

  /**
   * Helper method that removes an object from the indices.
   *
   * @param object the object to be removed
   */
  private void removeFromIndices(T object) {
    for (Map.Entry<FieldIndex<T>, ConcurrentHashMap<Object, ConcurrentHashSet<T>>> fieldInfo :
        mIndexMap.entrySet()) {
      Object fieldValue = fieldInfo.getKey().getFieldValue(object);
      ConcurrentHashMap<Object, ConcurrentHashSet<T>> index = fieldInfo.getValue();
      ConcurrentHashSet<T> objSet = index.get(fieldValue);
      if (objSet != null) {
        objSet.remove(object);
      }
    }
  }

  /**
   * Removes the subset of objects with the specified field value.
   *
   * @param index the field index
   * @param value the field value
   * @return the number of objects removed
   */
  public int removeByField(FieldIndex<T> index, Object value) {
    ConcurrentHashSet<T> toRemove = getByFieldInternal(index, value);
    if (toRemove == null) {
      return 0;
    }
    int removed = 0;
    for (T o : toRemove) {
      if (remove(o)) {
        removed++;
      }
    }
    return removed;
  }

  /**
   * @return the number of objects in this indexed set (O(1) time)
   */
  public int size() {
    return mObjects.size();
  }

  /**
   * Gets the set of objects with the specified field value - internal function.
   *
   * @param index the field index
   * @param value the field value
   * @return the set of objects with the specified field value
   */
  private ConcurrentHashSet<T> getByFieldInternal(FieldIndex<T> index, Object value) {
    return mIndexMap.get(index).get(value);
  }
}
