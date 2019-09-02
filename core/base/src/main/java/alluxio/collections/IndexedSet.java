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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A set of objects that are indexed and thus can be queried by specific fields of the object.
 * Different {@link IndexedSet} instances may specify different fields to index. The field type
 * must be comparable. The field value must not be changed after an object is added to the set,
 * otherwise, behavior for all operations is not specified.
 *
 * If concurrent adds or removes for objects which are equivalent, but not the same exact object,
 * the behavior is undefined. Therefore, do not add or remove "clones" objects in the
 * {@link IndexedSet}.
 *
 * <p>
 * Example usage:
 *
 * We have a set of puppies:
 *
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
 *  IndexDefinition<Puppy> idIndex = new IndexDefinition<Puppy>(true) {
 *    {@literal @Override}
 *    Object getFieldValue(Puppy o) {
 *      return o.id();
 *    }
 *  }
 *
 *  IndexDefinition<Puppy> nameIndex = new IndexDefinition<Puppy>(true) {
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
public class IndexedSet<T> extends AbstractSet<T> {
  /**
   * The first index of the indexed set. This index is used to guarantee uniqueness of objects,
   * support iterator and provide quick lookup.
   */
  private final FieldIndex<T, ?> mPrimaryIndex;
  /**
   * Map from index definition to the index. An index is a map from index value to one or a set of
   * objects with that index value.
   */
  private final Map<IndexDefinition<T, ?>, FieldIndex<T, ?>> mIndices;

  /**
   * Constructs a new {@link IndexedSet} instance with at least one field as the index.
   *
   * @param primaryIndexDefinition at least one field is needed to index the set of objects. This
   *        primaryIndexDefinition is used to initialize {@link IndexedSet#mPrimaryIndex} and is
   *        recommended to be unique in consideration of performance.
   * @param otherIndexDefinitions other index definitions to index the set
   */
  @SafeVarargs
  public IndexedSet(IndexDefinition<T, ?> primaryIndexDefinition,
      IndexDefinition<T, ?>... otherIndexDefinitions) {
    Iterable<IndexDefinition<T, ?>> indexDefinitions =
        Iterables.concat(Collections.singletonList(primaryIndexDefinition),
            Arrays.asList(otherIndexDefinitions));

    // initialization
    Map<IndexDefinition<T, ?>, FieldIndex<T, ?>> indices = new HashMap<>();

    for (IndexDefinition<T, ?> indexDefinition : indexDefinitions) {
      FieldIndex<T, ?> index = indexDefinition.isUnique()
          ? new UniqueFieldIndex<>(indexDefinition) : new NonUniqueFieldIndex<>(indexDefinition);

      indices.put(indexDefinition, index);
    }

    mPrimaryIndex = indices.get(primaryIndexDefinition);
    mIndices = Collections.unmodifiableMap(indices);
  }

  /**
   * Removes all the entries in this set.
   *
   * This is an expensive operation, and concurrent adds are permitted.
   */
  @Override
  public void clear() {
    for (T obj : mPrimaryIndex) {
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
  @Override
  public boolean add(T object) {
    Preconditions.checkNotNull(object, "object");

    // Locking this object protects against removing the exact object, but does not protect against
    // removing a distinct, but equivalent object.
    synchronized (object) {
      // add() will atomically add the object to the index, if it doesn't exist.
      if (!mPrimaryIndex.add(object)) {
        // This object is already added, possibly by another concurrent thread.
        return false;
      }

      for (FieldIndex<T, ?> fieldIndex : mIndices.values()) {
        fieldIndex.add(object);
      }
    }
    return true;
  }

  /**
   * Returns an iterator over the elements in this set. The elements are returned in no particular
   * order. It is to implement {@link Iterable} so that users can foreach the {@link IndexedSet}
   * directly.
   *
   * Note that the behavior of the iterator is unspecified if the underlying collection is
   * modified while a thread is going through the iterator.
   *
   * @return an iterator over the elements in this {@link IndexedSet}
   */
  @Override
  public Iterator<T> iterator() {
    return new IndexedSetIterator();
  }

  /**
   * Specialized iterator for {@link IndexedSet}.
   *
   * This is needed to support consistent removal from the set and the indices.
   */
  private class IndexedSetIterator implements Iterator<T> {
    private final Iterator<T> mSetIterator;
    private T mObject;

    public IndexedSetIterator() {
      mSetIterator = mPrimaryIndex.iterator();
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
   * Whether there is an object with the specified index field value in the set.
   *
   * @param indexDefinition the field index definition
   * @param value the field value
   * @param <V> the field type
   * @return true if there is one such object, otherwise false
   */
  public <V> boolean contains(IndexDefinition<T, V> indexDefinition, V value) {
    FieldIndex<T, V> index = (FieldIndex<T, V>) mIndices.get(indexDefinition);
    if (index == null) {
      throw new IllegalStateException("the given index isn't defined for this IndexedSet");
    }
    return index.containsField(value);
  }

  /**
   * Gets a subset of objects with the specified field value. If there is no object with the
   * specified field value, a newly created empty set is returned.
   *
   * @param indexDefinition the field index definition
   * @param value the field value to be satisfied
   * @param <V> the field type
   * @return the set of objects or an empty set if no such object exists
   */
  public <V> Set<T> getByField(IndexDefinition<T, V> indexDefinition, V value) {
    FieldIndex<T, V> index = (FieldIndex<T, V>) mIndices.get(indexDefinition);
    if (index == null) {
      throw new IllegalStateException("the given index isn't defined for this IndexedSet");
    }
    return index.getByField(value);
  }

  /**
   * Gets the object from the set of objects with the specified field value.
   *
   * @param indexDefinition the field index definition
   * @param value the field value
   * @param <V> the field type
   * @return the object or null if there is no such object
   */
  public <V> T getFirstByField(IndexDefinition<T, V> indexDefinition, V value) {
    FieldIndex<T, V> index = (FieldIndex<T, V>) mIndices.get(indexDefinition);
    if (index == null) {
      throw new IllegalStateException("the given index isn't defined for this IndexedSet");
    }
    return index.getFirst(value);
  }

  /**
   * Removes an object from the set.
   *
   * @param object the object to remove
   * @return true if the object is in the set and removed successfully, otherwise false
   */
  @Override
  public boolean remove(Object object) {
    if (object == null) {
      return false;
    }
    // Locking this object protects against removing the exact object that might be in the
    // process of being added, but does not protect against removing a distinct, but equivalent
    // object.
    synchronized (object) {
      if (mPrimaryIndex.containsObject((T) object)) {
        // This isn't technically typesafe. However, given that success is true, it's very unlikely
        // that the object passed to remove is not of type <T>.
        @SuppressWarnings("unchecked")
        T tObj = (T) object;
        removeFromIndices(tObj);
        return true;
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
    for (FieldIndex<T, ?> fieldValue : mIndices.values()) {
      fieldValue.remove(object);
    }
  }

  /**
   * Removes the subset of objects with the specified index field value.
   *
   * @param indexDefinition the field index
   * @param value the field value
   * @param <V> the field type
   * @return the number of objects removed
   */
  public <V> int removeByField(IndexDefinition<T, V> indexDefinition, V value) {
    Set<T> toRemove = getByField(indexDefinition, value);

    int removed = 0;
    for (T o : toRemove) {
      if (remove(o)) {
        removed++;
      }
    }
    return removed;
  }

  /**
   * @return the number of objects in this indexed set
   */
  @Override
  public int size() {
    return mPrimaryIndex.size();
  }
}
