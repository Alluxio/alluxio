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
