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

import java.util.List;

/**
 * An interface for provide nodes.
 * @param <T> the type of node
 */
public interface NodeProvider<T> {

  /**
   * @param identifier   an unique identifier used to obtain the nodes
   * @param count        how many desirable nodes to be returned
   * @return a list of the chosen nodes by specific hash function
   */
  List<T> get(Object identifier, int count);

  /**
   * Refresh the nodes.
   *
   * @param nodes the new nodes
   */
  void refresh(List<T> nodes);
}
