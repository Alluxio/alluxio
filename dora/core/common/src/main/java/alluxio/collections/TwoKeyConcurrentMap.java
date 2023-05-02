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

import static java.util.stream.Collectors.toSet;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * A two-level concurrent map implementation. Concurrent usage is managed by the outer map.
 *
 * Users can supply the inner map type of their choice.
 *
 * @param <K1> the first key type
 * @param <K2> the second key type
 * @param <V> the value type
 * @param <M> the type for the inner map
 */
public class TwoKeyConcurrentMap<K1, K2, V, M extends Map<K2, V>> extends ConcurrentHashMap<K1, M> {
  private static final long serialVersionUID = 1L;

  private final Supplier<M> mInnerMapFn;

  /**
   * @param innerMapCreator supplier for the inner map type
   */
  public TwoKeyConcurrentMap(Supplier<M> innerMapCreator) {
    mInnerMapFn = innerMapCreator;
  }

  /**
   * @param k1 the first key
   * @param k2 the second key
   * @param v the value
   */
  public void addInnerValue(K1 k1, K2 k2, V v) {
    compute(k1, (k, inner) -> {
      if (inner == null) {
        inner = mInnerMapFn.get();
      }
      inner.put(k2, v);
      return inner;
    });
  }

  /**
   * @param k1 the first key
   * @param k2 the second key
   */
  @Nullable
  public void removeInnerValue(K1 k1, K2 k2) {
    computeIfPresent(k1, (k, inner) -> {
      inner.remove(k2);
      if (inner.isEmpty()) {
        return null;
      }
      return inner;
    });
  }

  /**
   * Flattens the (key1, key2, value) triples according to the given function.
   *
   * @param fn a function to create an entry from the keys and value
   * @param <R> the result type of the function
   * @return the set of entries
   */
  public <R> Set<R> flattenEntries(TriFunction<K1, K2, V, R> fn) {
    return entrySet().stream()
        .flatMap(outer -> outer.getValue().entrySet().stream()
            .map(inner -> fn.apply(outer.getKey(), inner.getKey(), inner.getValue())))
        .collect(toSet());
  }

  /**
   * A function with three arguments.
   *
   * @param <A> the first argument
   * @param <B> the second argument
   * @param <C> the third argument
   * @param <R> the return type
   */
  @FunctionalInterface
  public interface TriFunction<A, B, C, R> {
    /**
     * @param a the first argument
     * @param b the second argument
     * @param c the third argument
     * @return the result
     */
    R apply(A a, B b, C c);
  }

  /**
   * The equals implementation for this map simply uses the superclass's equals.
   */
  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  /**
   * The hashCode implementation for this map simply uses the superclass's.
   */
  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
