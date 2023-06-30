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

package alluxio.node;

import static java.lang.String.format;

import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;

import java.util.List;
import java.util.function.Function;

public interface NodeProvider<T> {
  List<T> get(Object identifier, int count);

  void refresh(List<T> nodes);

  class Factory {
    public static <T> NodeProvider createActiveNodeProvider(
        AlluxioConfiguration alluxioConfiguration,
        NodeSelectionHashStrategy nodeSelectionHashStrategy,
        List<T> activeNodes,
        Function<T, String> identifierFunction,
        Function<Pair<List<T>, List<T>>, Boolean> nodeUpdateFunction) {
      switch (nodeSelectionHashStrategy) {
        case MODULAR_HASHING:
          return new ModularHashingNodeProvider(activeNodes);
        case CONSISTENT_HASHING:
          int minVirtualNodeCount = 2000;
          return ConsistentHashingNodeProvider.create(activeNodes, minVirtualNodeCount,
              identifierFunction, nodeUpdateFunction);
        default:
          throw new IllegalArgumentException(
              format("Unknown NodeSelectionHashStrategy: %s", nodeSelectionHashStrategy));
      }
    }
  }
}
