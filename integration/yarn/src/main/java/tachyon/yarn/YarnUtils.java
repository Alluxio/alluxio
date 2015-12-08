/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.yarn;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * Utility methods for working with Yarn.
 */
public final class YarnUtils {
  private static final NodeState[] USABLE_NODE_STATES;
  static {
    List<NodeState> usableStates = Lists.newArrayList();
    for (NodeState nodeState : NodeState.values()) {
      if (!nodeState.isUnusable()) {
        usableStates.add(nodeState);
      }
    }
    USABLE_NODE_STATES = usableStates.toArray(new NodeState[usableStates.size()]);
  }

  /**
   * Returns the set of host names for all nodes yarnClient's yarn cluster.
   *
   * @param yarnClient the client to use to look up node information
   * @return the set of host names
   * @throws YarnException if an error occurs within Yarn
   * @throws IOException if an error occurs in Yarn's underlying IO
   */
  public static Set<String> getNodeHosts(YarnClient yarnClient) throws YarnException, IOException {
    ImmutableSet.Builder<String> nodeHosts = ImmutableSet.builder();
    for (NodeReport runningNode : yarnClient.getNodeReports(USABLE_NODE_STATES)) {
      nodeHosts.add(runningNode.getNodeId().getHost());
    }
    return nodeHosts.build();
  }

  private YarnUtils() {} // this utils class should not be instantiated
}
