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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.TestLoggerRule;
import alluxio.hub.proto.AlluxioNodeStatus;
import alluxio.hub.proto.AlluxioNodeStatusOrBuilder;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.proto.AlluxioProcessStatus;
import alluxio.hub.proto.ProcessState;
import alluxio.hub.test.BaseHubTest;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.UUID;

public class AlluxioClusterTest extends BaseHubTest {

  private AlluxioCluster mCluster;

  @Rule
  public TestLoggerRule mLogger = new TestLoggerRule();

  @Before
  public void before() {
    mCluster = new AlluxioCluster(MoreExecutors.newDirectExecutorService());
  }

  @Test
  public void testAdd() {
    AlluxioNodeStatusOrBuilder s = AlluxioNodeStatus.newBuilder()
        .setHostname("test-host")
        .addProcess(AlluxioProcessStatus.newBuilder()
            .setState(ProcessState.STARTING)
            .setNodeType(AlluxioNodeType.MASTER));
    mCluster.heartbeat(s);
    assertEquals(1, mCluster.size());
    AlluxioNodeStatusOrBuilder s2 = AlluxioNodeStatus.newBuilder()
        .setHostname("test-host");
    mCluster.heartbeat(s2);
    assertEquals(1, mCluster.size());
    AlluxioNodeStatusOrBuilder s3 = AlluxioNodeStatus.newBuilder()
        .setHostname("test-host-2");
    mCluster.heartbeat(s3);
    assertEquals(2, mCluster.size());
  }

  @Test
  public void testToProtoEmpty() {
    alluxio.hub.proto.AlluxioCluster cluster =
        new AlluxioCluster(MoreExecutors.newDirectExecutorService()).toProto();
    List<AlluxioNodeStatus> l = cluster.getNodeList();
    assertEquals(0, l.size());
  }

  @Test
  public void testToProtoNotEmpty() {
    int limit = 4;
    new Random().ints(limit)
        .mapToObj(i -> generateNodeStatus())
        .forEach(mCluster::heartbeat);
    alluxio.hub.proto.AlluxioCluster proto = mCluster.toProto();
    assertEquals(limit, proto.getNodeCount());
    proto.getNodeList().forEach(node -> {
      assertTrue(node.hasHostname());
      node.getProcessList().forEach(stat -> {
        assertTrue(stat.hasNodeType());
        assertTrue(stat.hasState());
      });
    });
  }

  @Test
  public void testRemove() {
    AlluxioNodeStatusOrBuilder s = generateNodeStatus();
    mCluster.heartbeat(s);
    assertEquals(1, mCluster.size());
    mCluster.remove(s);
    assertEquals(0, mCluster.size());
    mCluster.heartbeat(s);
    assertEquals(1, mCluster.size());
    mCluster.remove(s.getHostname());
    assertEquals(0, mCluster.size());
  }

  private static AlluxioNodeStatusOrBuilder generateNodeStatus() {
    return AlluxioNodeStatus.newBuilder()
        .setHostname(UUID.randomUUID().toString())
        .addProcess(AlluxioProcessStatus.newBuilder()
            .setNodeType(AlluxioNodeType.values()[
                new Random().nextInt(AlluxioNodeType.values().length)])
            .setState(ProcessState.forNumber(
                new Random().nextInt(ProcessState.values().length)))
        );
  }
}
