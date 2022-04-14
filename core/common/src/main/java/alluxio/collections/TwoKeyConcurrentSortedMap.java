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

import java.util.SortedMap;
import java.util.function.Supplier;

/**
 * A two-level concurrent map implementation where the inner map is sorted.
 * Concurrent usage is managed by the outer map.
 *
 * Users can supply the inner sorted map type of their choice.
 *
 * @param <K1> the first key type
 * @param <K2> the second key type
 * @param <V> the value type
 * @param <M> the type for the inner sorted map
 */

public class TwoKeyConcurrentSortedMap<K1, K2, V, M extends SortedMap<K2, V>>
    extends TwoKeyConcurrentMap<K1, K2, V, M> {
  /**
   * @param innerMapCreator supplier for the inner map type
   */
  public TwoKeyConcurrentSortedMap(Supplier<M> innerMapCreator) {
    super(innerMapCreator);
  }
}
