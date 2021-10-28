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

package alluxio.hub.manager.util;

import alluxio.hub.proto.AlluxioNodeStatus;
import alluxio.hub.proto.AlluxioNodeStatusOrBuilder;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.proto.AlluxioProcessStatus;
import alluxio.hub.proto.HubNodeAddress;
import alluxio.hub.proto.ProcessState;

import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test utility functions related to hub objects.
 */
public class HubTestUtils {
  private static final Random RAND = new Random();

  /**
   * @return a randomly populated hub node
   */
  public static HubNodeAddress generateNodeAddress() {
    return HubNodeAddress.newBuilder().setHostname(UUID.randomUUID().toString())
        .setRpcPort(RAND.nextInt(65535)).build();
  }

  /**
   * Convert a hub node address to a set of Alluxio process statuses.
   *
   * @param addr the hub address the alluxio statuses come from
   * @param nodeTypes the set of Alluxio node types returned in the list of statuses
   *
   * @return a set of Alluxio node statuses
   */
  public static AlluxioNodeStatusOrBuilder hubStatusToAlluxioStatus(HubNodeAddress addr,
      AlluxioNodeType... nodeTypes) {
    return AlluxioNodeStatus.newBuilder()
        .addAllProcess(Stream.of(nodeTypes)
            .map(nt -> AlluxioProcessStatus.newBuilder()
                .setNodeType(nt)
                .setState(ProcessState.RUNNING)
                .build())
            .collect(Collectors.toList()))
        .setHostname(addr.getHostname());
  }
}
