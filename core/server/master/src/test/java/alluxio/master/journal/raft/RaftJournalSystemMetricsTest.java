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

package alluxio.master.journal.raft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

/**
 * Units tests for {@link RaftJournalSystem}'s metrics.
 */
public final class RaftJournalSystemMetricsTest {

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @After
  public void after() {
    Configuration.reloadProperties();
  }

  @Test
  public void journalStateMachineMetrics() throws Exception {
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES,
        "localhost:19200,localhost:19201,localhost:19202");
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT, 19200);
    RaftJournalSystem system =
        new RaftJournalSystem(mFolder.newFolder().toURI(), ServiceType.MASTER_RAFT);
    String[] metricsNames = new String[] {
        MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_LAST_INDEX.getName(),
        MetricKey.MASTER_JOURNAL_ENTRIES_SINCE_CHECKPOINT.getName(),
        MetricKey.MASTER_JOURNAL_LAST_CHECKPOINT_TIME.getName(),
        MetricKey.MASTER_JOURNAL_LAST_APPLIED_COMMIT_INDEX.getName(),
        MetricKey.MASTER_JOURNAL_CHECKPOINT_WARN.getName(),
    };
    JournalStateMachine stateMachine = new JournalStateMachine(system.getJournals(), system);
    for (String name : metricsNames) {
      assertNotNull(MetricsSystem.METRIC_REGISTRY.getGauges().get(name));
    }
    stateMachine.close();
    for (String name : metricsNames) {
      assertNull(MetricsSystem.METRIC_REGISTRY.getGauges().get(name));
    }
    JournalStateMachine newStateMachine = new JournalStateMachine(system.getJournals(), system);
    for (String name : metricsNames) {
      assertNotNull(MetricsSystem.METRIC_REGISTRY.getGauges().get(name));
    }
    newStateMachine.close();
    for (String name : metricsNames) {
      assertNull(MetricsSystem.METRIC_REGISTRY.getGauges().get(name));
    }
  }

  @Test
  public void metrics() throws Exception {
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES,
        "localhost:19200,localhost:19201,localhost:19202");
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT, 19200);
    RaftJournalSystem raftJournalSystem =
        new RaftJournalSystem(mFolder.newFolder().toURI(), ServiceType.MASTER_RAFT);
    RaftJournalSystem system = Mockito.spy(raftJournalSystem);
    RaftProtos.RoleInfoProto leaderInfo = RaftProtos.RoleInfoProto.newBuilder()
        .setRole(RaftProtos.RaftPeerRole.LEADER).build();
    RaftProtos.RoleInfoProto followerInfo = RaftProtos.RoleInfoProto.newBuilder()
        .setRole(RaftProtos.RaftPeerRole.FOLLOWER)
        .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
            .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder()
                .setId(RaftProtos.RaftPeerProto.newBuilder()
                    .setId(ByteString.copyFromUtf8("localhost_19201")))))
        .build();

    Map<String, Long> sn1 = new HashMap<String, Long>() {
      {
        put("foo", 1L);
      }
    };
    Mockito.doReturn(sn1).when(system).getCurrentSequenceNumbers();
    system.startInternal();
    Mockito.doReturn(null).when(system).getRaftRoleInfo();
    assertEquals(-1, getClusterLeaderIndex());
    assertEquals(-1, getMasterRoleId());
    assertEquals("WAITING_FOR_ELECTION", getClusterLeaderId());
    assertEquals(sn1, getMasterJournalSequenceNumbers(system));

    Map<String, Long> sn2 = new HashMap<String, Long>() {
      {
        put("foo", 1L);
        put("bar", 2L);
      }
    };
    Mockito.doReturn(sn2).when(system).getCurrentSequenceNumbers();
    system.gainPrimacy();
    Mockito.doReturn(leaderInfo).when(system).getRaftRoleInfo();
    assertEquals(0, getClusterLeaderIndex());
    assertEquals(RaftProtos.RaftPeerRole.LEADER_VALUE, getMasterRoleId());
    assertEquals(system.getLocalPeerId().toString(), getClusterLeaderId());
    assertEquals(sn2, getMasterJournalSequenceNumbers(system));

    Map<String, Long> sn3 = new HashMap<String, Long>() {
      {
        put("foo", 1L);
        put("bar", 2L);
        put("baz", 3L);
      }
    };
    Mockito.doReturn(sn3).when(system).getCurrentSequenceNumbers();
    system.losePrimacy();
    Mockito.doReturn(followerInfo).when(system).getRaftRoleInfo();
    assertEquals(1, getClusterLeaderIndex());
    assertEquals(RaftProtos.RaftPeerRole.FOLLOWER_VALUE, getMasterRoleId());
    assertEquals("localhost_19201", getClusterLeaderId());
    assertEquals(sn3, getMasterJournalSequenceNumbers(system));

    Map<String, Long> sn4 = new HashMap<String, Long>() {
      {
        put("foo", 1L);
        put("bar", 2L);
        put("baz", 3L);
        put("qux", 4L);
      }
    };
    Mockito.doReturn(sn4).when(system).getCurrentSequenceNumbers();
    system.gainPrimacy();
    Mockito.doReturn(leaderInfo).when(system).getRaftRoleInfo();
    assertEquals(0, getClusterLeaderIndex());
    assertEquals(RaftProtos.RaftPeerRole.LEADER_VALUE, getMasterRoleId());
    assertEquals(system.getLocalPeerId().toString(), getClusterLeaderId());
    assertEquals(sn4, getMasterJournalSequenceNumbers(system));
  }

  private static int getClusterLeaderIndex() {
    return (int) MetricsSystem.METRIC_REGISTRY.getGauges()
        .get(MetricKey.CLUSTER_LEADER_INDEX.getName()).getValue();
  }

  private static int getMasterRoleId() {
    return (int) MetricsSystem.METRIC_REGISTRY.getGauges()
        .get(MetricKey.MASTER_ROLE_ID.getName()).getValue();
  }

  private static String getClusterLeaderId() {
    return (String) MetricsSystem.METRIC_REGISTRY.getGauges()
        .get(MetricKey.CLUSTER_LEADER_ID.getName()).getValue();
  }

  private static Map<String, Long> getMasterJournalSequenceNumbers(RaftJournalSystem system) {
    Map<String, Long> sequenceNumber = system.getCurrentSequenceNumbers();
    Map<String, Long> result = new HashMap<String, Long>();
    for (String masterName : sequenceNumber.keySet()) {
      long value = (long) MetricsSystem.METRIC_REGISTRY.getGauges()
          .get(MetricKey.MASTER_JOURNAL_SEQUENCE_NUMBER.getName() + "." + masterName).getValue();
      result.put(masterName, value);
    }
    return result;
  }
}
