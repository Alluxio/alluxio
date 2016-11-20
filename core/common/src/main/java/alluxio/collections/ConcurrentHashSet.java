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

import io.netty.util.internal.chmv8.ConcurrentHashMapV8;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A concurrent hash set. This is backed by a {@link ConcurrentHashMapV8}, and {@link Set}
 * operations are translated to {@link ConcurrentHashMapV8} operations.
 *
 * @param <T> the type of the set objects
 */
@ThreadSafe
public final class ConcurrentHashSet<T> extends AbstractSet<T> {
  // COMPATIBILITY: This field needs to declared as Map (as opposed to ConcurrentHashMap). The
  // reason is that the return type of ConcurrentHashMapV8#keySet() has changed from Set<K> to
  // KeySetView<K,V> between Java 7 and Java 8 and this can result in a NoSuchMethod runtime
  // exception when using Java 7 to run byte code compiled with Java 8 (even if the compiler is
  // told to compile for Java 7).
  //
  // See: https://gist.github.com/AlainODea/1375759b8720a3f9f094
  private final Map<T, Boolean> mMap;

  /**
   * Creates a new {@link ConcurrentHashSet}.
   */
  public ConcurrentHashSet() {
    this(2, 0.95f, 1);
  }

  /**
   * Creates a new {@link ConcurrentHashSet}.
   *
   * @param initialCapacity the initial capacity
   * @param loadFactor the load factor threshold, used to control resizing
   * @param concurrencyLevel the estimated number of concurrently updating threads
   */
  public ConcurrentHashSet(int initialCapacity, float loadFactor, int concurrencyLevel) {
    mMap = new ConcurrentHashMapV8<>(initialCapacity, loadFactor, concurrencyLevel);
  }

  @Override
  public Iterator<T> iterator() {
    return mMap.keySet().iterator();
  }

  @Override
  public int size() {
    return mMap.size();
  }

  @Override
  public boolean add(T element) {
    return mMap.put(element, Boolean.TRUE) == null;
  }

  /**
   * Adds an element into the set, if and only if it is not already a part of the set.
   *
   * @param element the element to add into the set
   * @return true if this set did not already contain the specified element
   */
  public boolean addIfAbsent(T element) {
    // COMPATIBILITY: We need to cast mMap to ConcurrentHashMapV8 to make sure the code can compile
    // on Java 7 because the Map#putIfAbsent() method has only been introduced in Java 8.
    return ((ConcurrentHashMapV8<T, Boolean>) mMap).putIfAbsent(element, Boolean.TRUE) == null;
  }

  @Override
  public void clear() {
    mMap.clear();
  }

  @Override
  public boolean contains(Object o) {
    return mMap.containsKey(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return mMap.keySet().containsAll(c);
  }

  @Override
  public boolean isEmpty() {
    return mMap.isEmpty();
  }

  @Override
  public boolean remove(Object o) {
    return mMap.remove(o) != null;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return mMap.keySet().removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return mMap.keySet().retainAll(c);
  }

  @Override
  public Object[] toArray() {
    return mMap.keySet().toArray();
  }

  @Override
  public <E> E[] toArray(E[] a) {
    return mMap.keySet().toArray(a);
  }

  @Override
  public String toString() {
    return mMap.keySet().toString();
  }
}
