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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * An implementation {@link ConcurrentHashMap} that utilizes identity semantics
 * with its keys
 *
 * Having identity semantics means that it is possible to have two objects with which are
 * equal based on the object's {@code equals()} implementation, but not be overridden in the hash
 * map if you try to put both. For example:
 *
 * <pre>
 *   ConcurrentIdentityHashMap&lt;String, Boolean&gt; map = new ConcurrentIdentityHashMap&lt;&gt;();
 *   String s1 = new String("test");
 *   String s2 = new String("test");
 *   map.put(s1, true);
 *   map.put(s2, false);
 *   map.size(); // Prints "2".
 *   map.get(s1); // true
 *   map.get(s2); // false
 * </pre>
 *
 * Keys in this mapped are hashed based on the identity, that is, the location in memory of the
 * objects rather than the equality which java typically uses for comparison.
 *
 * This implementation of {@link ConcurrentMap} has a few limitations which the corresponding
 * {@link ConcurrentHashMap} does not have.
 *
 * - This map's {@link #entrySet()} returns a copy of the entries at the time of calling the method.
 * The returned set does will not receive updates from the underlying map. This is a departure
 * from the behavior of {@link ConcurrentHashMap}
 *
 * - This map's {@link #keySet()} does return the map-backed keySet and provides the same
 * behavior as in {@link ConcurrentHashMap}, however the difference is that {@link Set#toArray()}
 * and {@link Set#toArray(Object[])} methods are left unimplemented and will throw an error if
 * the user attempts to convert the set into an array.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
@ThreadSafe
public class ConcurrentIdentityHashMap<K, V> implements ConcurrentMap<K, V> {
  private static final String UNSUPPORTED_OP_FMT = "%s is not supported in this set returned from "
          + ConcurrentIdentityHashMap.class.getCanonicalName();

  private final ConcurrentHashMap<IdentityObject<K>, V> mInternalMap;

  /**
   * Creates a new, empty map with the default initial table size (16).
   */
  public ConcurrentIdentityHashMap() {
    mInternalMap = new ConcurrentHashMap<>();
  }

  /**
   * Creates a new, empty map with an initial table size accommodating the specified number of
   * elements without having to dynamically resize.
   *
   * @param initialCapacity The implementation performs internal sizing to accommodate this many
   *        elements.
   * @throws IllegalArgumentException if the initial capacity of
   *         elements is negative
   */
  public ConcurrentIdentityHashMap(int initialCapacity) {
    mInternalMap = new ConcurrentHashMap<>(initialCapacity);
  }

  /**
   * Creates a new, empty map with an initial table size based on the given number of elements
   * ({@code initialCapacity}) and initial load factor ({@code loadFactor}) with a
   * {@code concurrencyLevel} of 1.
   *
   * @param initialCapacity the initial capacity. The implementation performs internal sizing to
   *        accommodate this many elements, given the specified load factor.
   * @param loadFactor the load factor (table density) for
   *        establishing the initial table size
   * @throws IllegalArgumentException if the initial capacity of
   * elements is negative or the load factor is nonpositive
   */
  public ConcurrentIdentityHashMap(int initialCapacity, float loadFactor) {
    this(initialCapacity, loadFactor, 1);
  }

  /**
   * Creates a new, empty map with an initial table size based on the given number of elements
   * ({@code initialCapacity}), load factor ({@code loadFactor}), and number of
   * expected concurrently updating threads ({@code concurrencyLevel}).
   *
   * @param initialCapacity the initial capacity. The implementation performs internal sizing to
   *        accommodate this many elements, given the specified load factor.
   * @param loadFactor the load factor (table density) for
   *        establishing the initial table size
   * @param concurrencyLevel the estimated number of concurrently updating threads. The
   *        implementation may use this value as a sizing hint.
   * @throws IllegalArgumentException if the initial capacity is negative or the load factor or
   *         concurrencyLevel are nonpositive
   */
  public ConcurrentIdentityHashMap(int initialCapacity,
      float loadFactor, int concurrencyLevel) {
    mInternalMap = new ConcurrentHashMap<>(initialCapacity, loadFactor, concurrencyLevel);
  }

  @Override
  public boolean containsKey(Object k) {
    return mInternalMap.containsKey(new IdentityObject<>(k));
  }

  @Override
  public V put(K k, V v) {
    return mInternalMap.put(new IdentityObject<>(k), v);
  }

  @Override
  public V putIfAbsent(K k, V v) {
    return mInternalMap.putIfAbsent(new IdentityObject<>(k), v);
  }

  @Override
  public boolean remove(Object k, Object v) {
    return mInternalMap.remove(new IdentityObject<>(k), v);
  }

  @Override
  public V remove(Object k) {
    return mInternalMap.remove(new IdentityObject<>(k));
  }

  @Override
  public boolean replace(K k, V v1, V v2) {
    return mInternalMap.replace(new IdentityObject<>(k), v1, v2);
  }

  @Override
  public V replace(K k, V v) {
    return mInternalMap.replace(new IdentityObject<>(k), v);
  }

  @Override
  public V get(Object k) {
    return mInternalMap.get(new IdentityObject<>(k));
  }

  @Override
  public Collection<V> values() {
    return mInternalMap.values();
  }

  @Override
  public void clear() {
    mInternalMap.clear();
  }

  /**
   * Returns a set representing the entries of this map at the time of calling.
   *
   * Note, this method will create a copy of the map's entry set at creation time. Standard
   * behavior in the {@link ConcurrentHashMap} says that the entry set backed by the map is
   * updated when the underlying map is updated. That is not the case for this class.
   *
   * @return The set of entries in the map at time of calling
   */
  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return mInternalMap.entrySet().stream().map(IdentityEntry<K, V>::new)
        .collect(Collectors.toSet());
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    map.forEach((k, v) -> mInternalMap.put(new IdentityObject<>(k), v));
  }

  @Override
  public boolean isEmpty() {
    return mInternalMap.isEmpty();
  }

  @Override
  public int size() {
    return mInternalMap.size();
  }

  @Override
  public boolean containsValue(Object value) {
    return mInternalMap.containsValue(value);
  }

  /**
   * Returns a set representing all keys contained within the map.
   *
   * Note this method departs slightly from the standard {@link ConcurrentHashMap} implementation
   * by providing its own implementation of a Set for {@link IdentityObject}. Not all methods
   * have been implemented. Namely the {@code toArray}, {@code add*}, and
   * {@code *all(Collection<?> c)} methods.
   *
   * @return The set of keys in the map
   */
  @Override
  public Set<K> keySet() {
    Set<IdentityObject<K>> set = mInternalMap.keySet();
    return new Set<K>() {

      @Override
      public int size() {
        return set.size();
      }

      @Override
      public boolean isEmpty() {
        return set.isEmpty();
      }

      @Override
      public boolean contains(Object o) {
        return set.contains(new IdentityObject<>(o));
      }

      @Override
      public Iterator<K> iterator() {
        return set.stream().map(IdentityObject::get).iterator();
      }

      @Override
      public Object[] toArray() {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_OP_FMT, "toArray"));
      }

      @Override
      public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_OP_FMT, "toArray"));
      }

      @Override
      public boolean add(K k) {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_OP_FMT, "add"));
      }

      @Override
      public boolean remove(Object o) {
        return set.remove(new IdentityObject<>(o));
      }

      /*
       * This method is possible to implement if inputs from the input collection are auto-boxed
       * into an IdentityObject<T> but the implementation has been left absent for now.
       */
      @Override
      public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_OP_FMT, "containsAll"));
      }

      @Override
      public boolean addAll(Collection<? extends K> c) {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_OP_FMT, "addAll"));
      }

      /*
       * This method is possible to implement if inputs from the input collection are auto-boxed
       * into an IdentityObject<T> but the implementation has been left absent for now.
       */
      @Override
      public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_OP_FMT, "retainAll"));
      }

      /*
       * This method is possible to implement if inputs from the input collection are auto-boxed
       * into an IdentityObject<T> but the implementation has been left absent for now.
       */
      @Override
      public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_OP_FMT, "removeAll"));
      }

      @Override
      public void clear() {
        set.clear();
      }
    };
  }

  private class IdentityObject<T> {
    private T mObj;

    public IdentityObject(T o) {
      mObj = o;
    }

    public T get() {
      return mObj;
    }

    /*
     * This method hashes on top of the {@link System#identityHashCode(Object)} because the
     * {@link ConcurrentHashMap} implementation uses a bucketing strategy where hash collisions
     * occur for when values do not differ in upper or lower bits. Using the extra hash here
     * helps ensure more even spread among the buckets of the internal map
     */
    @Override
    public int hashCode() {
      // Robert Jenkins 32-bit integer hash
      int a = System.identityHashCode(mObj);
      a = (a + 0x7ed55d16) + (a << 12);
      a = (a ^ 0xc761c23c) ^ (a >> 19);
      a = (a + 0x165667b1) + (a << 5);
      a = (a + 0xd3a2646c) ^ (a << 9);
      a = (a + 0xfd7046c5) + (a << 3);
      a = (a ^ 0xb55a4f09) ^ (a >> 16);
      return a;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IdentityObject)) {
        return false;
      }

      try {
        IdentityObject<T> other = (IdentityObject<T>) o;
        return mObj == other.mObj;
      } catch (ClassCastException e) {
        return false;
      }
    }
  }

  private class IdentityEntry<T, P> implements Map.Entry<T, P> {
    private final IdentityObject<T> mKey;
    private P mValue;

    IdentityEntry(Map.Entry<IdentityObject<T>, P> e) {
      mKey = e.getKey();
      mValue = e.getValue();
    }

    @Override
    public T getKey() {
      return mKey.get();
    }

    @Override
    public P getValue() {
      return mValue;
    }

    @Override
    public P setValue(P v) {
      P old = mValue;
      mValue = v;
      return old;
    }

    @Override
    public int hashCode() {
      return Objects.hash(mKey, mValue);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IdentityEntry)) {
        return false;
      }

      try {
        IdentityEntry<T, P> other = (IdentityEntry<T, P>) o;
        return mKey.equals(other.mKey)
            && mValue.equals(other.mValue);
      } catch (ClassCastException e) {
        return false;
      }
    }
  }
}
