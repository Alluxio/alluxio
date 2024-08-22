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

import alluxio.collections.FieldIndex;
import alluxio.collections.IndexDefinition;

import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A class representing a non-unique index using a PatriciaTrie. A non-unique index is an index
 * where an index value can map to one or more objects.
 *
 * @param <T> type of objects in this index
 * @param <V> type of the field used for indexing
 */
@NotThreadSafe
public class NonUniqueFieldIndexInTrie<T, V> implements FieldIndex<T, V> {
  private final IndexDefinition<T, V> mIndexDefinition;
  private final Trie<String, Set<T>> mIndexTrie;

  /**
   * Constructs a new {@link NonUniqueFieldIndexInTrie} instance.
   *
   * @param indexDefinition definition of index
   */
  public NonUniqueFieldIndexInTrie(IndexDefinition<T, V> indexDefinition) {
    // Wrap the PatriciaTrie with Collections.synchronizedMap to make it thread-safe
    mIndexTrie = new PatriciaTrie<>();
    mIndexDefinition = indexDefinition;
  }

  @Override
  public boolean add(T object) {
    AtomicBoolean res = new AtomicBoolean(false);
    V fieldValue = mIndexDefinition.getFieldValue(object);
    String fieldValueStr = fieldValue.toString();
    synchronized (mIndexTrie) { // Synchronize on the trie map to ensure thread safety
      mIndexTrie.compute(fieldValueStr, (key, set) -> {
        if (set == null) {
          set = Collections.newSetFromMap(new ConcurrentHashMap<>());
        }
        res.set(set.add(object));
        return set;
      });
    }
    return res.get();
  }

  @Override
  public boolean remove(T object) {
    AtomicBoolean res = new AtomicBoolean(false);
    V fieldValue = mIndexDefinition.getFieldValue(object);
    String fieldValueStr = fieldValue.toString();
    synchronized (mIndexTrie) { // Synchronize on the trie map to ensure thread safety
      mIndexTrie.computeIfPresent(fieldValueStr, (key, set) -> {
        res.set(set.remove(object));
        if (set.isEmpty()) {
          return null;
        }
        return set;
      });
    }
    return res.get();
  }

  @Override
  public void clear() {
    synchronized (mIndexTrie) { // Synchronize on the trie map to ensure thread safety
      mIndexTrie.clear();
    }
  }

  @Override
  public boolean containsField(V fieldValue) {
    synchronized (mIndexTrie) { // Synchronize on the trie map to ensure thread safety
      return mIndexTrie.containsKey(fieldValue);
    }
  }

  @Override
  public boolean containsObject(T object) {
    V fieldValue = mIndexDefinition.getFieldValue(object);
    synchronized (mIndexTrie) { // Synchronize on the trie map to ensure thread safety
      Set<T> set = mIndexTrie.get(fieldValue);
      return set != null && set.contains(object);
    }
  }

  @Override
  public Set<T> getByField(V value) {
    synchronized (mIndexTrie) { // Synchronize on the trie map to ensure thread safety
      Set<T> set = mIndexTrie.get(value);
      return set == null ? Collections.<T>emptySet() : set;
    }
  }

  @Override
  public T getFirst(V value) {
    synchronized (mIndexTrie) { // Synchronize on the trie map to ensure thread safety
      Set<T> all = mIndexTrie.get(value);
      return all == null ? null : all.iterator().next();
    }
  }

  @Override
  public Iterator<T> iterator() {
    synchronized (mIndexTrie) { // Synchronize on the trie map to ensure thread safety
      return new NonUniqueFieldIndexIterator();
    }
  }

  @Override
  public int size() {
    synchronized (mIndexTrie) { // Synchronize on the trie map to ensure thread safety
      int totalSize = 0;
      for (Set<T> set : mIndexTrie.values()) {
        totalSize += set.size();
      }
      return totalSize;
    }
  }

  /**
   * Specialized iterator for {@link NonUniqueFieldIndexInTrie}.
   *
   * This is needed to support consistent removal from the set and the indices.
   */
  private class NonUniqueFieldIndexIterator implements Iterator<T> {
    private final Iterator<Set<T>> mTrieIterator;
    private Iterator<T> mObjectIterator;
    private T mObject;

    public NonUniqueFieldIndexIterator() {
      synchronized (mIndexTrie) { // Synchronize on the trie map to ensure thread safety
        mTrieIterator = mIndexTrie.values().iterator();
        mObjectIterator = null;
        mObject = null;
      }
    }

    @Override
    public boolean hasNext() {
      synchronized (mIndexTrie) { // Synchronize on the trie map to ensure thread safety
        if (mObjectIterator != null && mObjectIterator.hasNext()) {
          return true;
        }
        while (mTrieIterator.hasNext()) {
          mObjectIterator = mTrieIterator.next().iterator();
          if (mObjectIterator.hasNext()) {
            return true;
          }
        }
        return false;
      }
    }

    @Override
    public T next() {
      synchronized (mIndexTrie) { // Synchronize on the trie map to ensure thread safety
        while (mObjectIterator == null || !mObjectIterator.hasNext()) {
          mObjectIterator = mTrieIterator.next().iterator();
        }
        mObject = mObjectIterator.next();
        return mObject;
      }
    }

    @Override
    public void remove() {
      synchronized (mIndexTrie) { // Synchronize on the trie map to ensure thread safety
        if (mObject != null) {
          NonUniqueFieldIndexInTrie.this.remove(mObject);
          mObject = null;
        } else {
          throw new IllegalStateException("next() was not called before remove()");
        }
      }
    }
  }
}
