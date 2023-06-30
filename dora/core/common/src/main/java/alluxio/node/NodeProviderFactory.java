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

/**
 * Node provider factory.
 */
public class NodeProviderFactory {

  /**
   * Create instance of {@link NodeProvider}.
   * @param alluxioConfiguration      the configuration
   * @param nodeSelectionHashStrategy the node selection hash strategy
   * @param nodes                     the nodes to select
   * @param identifierFunction the function to provide identifier
   * @param nodeUpdateFunction the function to check whether should update node
   * @return the instance of the {@link NodeProvider}
   * @param <T> the type of node
   */
  public static <T> NodeProvider createNodeProvider(
      AlluxioConfiguration alluxioConfiguration,
      NodeSelectionHashStrategy nodeSelectionHashStrategy,
      List<T> nodes,
      Function<T, String> identifierFunction,
      Function<Pair<List<T>, List<T>>, Boolean> nodeUpdateFunction) {
    switch (nodeSelectionHashStrategy) {
      case MODULAR_HASHING:
        return new ModularHashingNodeProvider(nodes);
      case CONSISTENT_HASHING:
        int minVirtualNodeCount = 2000;
        return ConsistentHashingNodeProvider.create(nodes, minVirtualNodeCount,
            identifierFunction, nodeUpdateFunction);
      default:
        throw new IllegalArgumentException(
            format("Unknown NodeSelectionHashStrategy: %s", nodeSelectionHashStrategy));
    }
  }
}