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

import java.util.TreeMap;

/**
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public class FixedSizeTreeMap<K, V> extends TreeMap<K, V> {
  private static final long serialVersionUID = 5953789792567225991L;

  private int mCapacity;

  /**
   * Construct an instance of {@link FixedSizeTreeMap}.
   * @param capacity the maximum element limitation
   */
  public FixedSizeTreeMap(int capacity) {
    mCapacity = capacity;
  }

  @Override
  public V put(K key, V value) {
    V oldValue = super.put(key, value);

    while (size() > mCapacity) {
      remove(firstKey());
    }

    return oldValue;
  }
}
