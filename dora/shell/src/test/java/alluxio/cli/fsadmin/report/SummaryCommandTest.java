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

package alluxio.cli.fsadmin.report;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.meta.MetaMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.MasterInfo;
import alluxio.grpc.MasterVersion;
import alluxio.grpc.NetAddress;
import alluxio.wire.BlockMasterInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SummaryCommandTest {

  private static AlluxioConfiguration sConf = Configuration.global();

  private MetaMasterClient mMetaMasterClient;
  private BlockMasterClient mBlockMasterClient;
  private ByteArrayOutputStream mOutputStream;
  private PrintStream mPrintStream;
  private MasterInfo mMasterInfo;

  @Before
  public void prepareBaseDependencies() throws IOException {
    // Generate random values for MasterInfo and BlockMasterInfo
    // Prepare mock meta master client
    mMetaMasterClient = mock(MetaMasterClient.class);
    mMasterInfo = MasterInfo.newBuilder()
        .setLeaderMasterAddress("testAddress")
        .setWebPort(1231)
        .setRpcPort(8462)
        .setStartTimeMs(1131242343122L)
        .setUpTimeMs(12412412312L)
        .setVersion("testVersion")
        .setSafeMode(false)
        .build();

    // Prepare mock block master client
    mBlockMasterClient = mock(BlockMasterClient.class);
    Map<String, Long> capacityBytesOnTiers = new HashMap<>();
    Map<String, Long> usedBytesOnTiers = new HashMap<>();
    capacityBytesOnTiers.put(Constants.MEDIUM_MEM, 1341353L);
    capacityBytesOnTiers.put("RAM", 23112L);
    capacityBytesOnTiers.put("DOM", 236501L);
    usedBytesOnTiers.put(Constants.MEDIUM_MEM, 62434L);
    usedBytesOnTiers.put("RAM", 6243L);
    usedBytesOnTiers.put("DOM", 74235L);
    BlockMasterInfo blockMasterInfo = new BlockMasterInfo()
        .setLiveWorkerNum(12)
        .setLostWorkerNum(4)
        .setCapacityBytes(1341353L)
        .setCapacityBytesOnTiers(capacityBytesOnTiers)
        .setUsedBytes(62434L)
        .setUsedBytesOnTiers(usedBytesOnTiers)
        .setFreeBytes(1278919L);
    when(mBlockMasterClient.getBlockMasterInfo(any()))
        .thenReturn(blockMasterInfo);

    // Prepare print stream
    mOutputStream = new ByteArrayOutputStream();
    mPrintStream = new PrintStream(mOutputStream, true, "utf-8");
  }

  @After
  public void after() {
    mPrintStream.close();
  }

  @Test
  public void ZkHaSummary() throws IOException {
    MasterVersion primaryVersion = MasterVersion.newBuilder()
        .setVersion(RuntimeConstants.VERSION).setState("Primary").setAddresses(
            NetAddress.newBuilder().setHost("hostname1").setRpcPort(10000).build()
        ).build();
    MasterVersion standby1Version = MasterVersion.newBuilder()
        .setVersion(RuntimeConstants.VERSION).setState("Standby").setAddresses(
            NetAddress.newBuilder().setHost("hostname2").setRpcPort(10001).build()
        ).build();
    MasterVersion standby2Version = MasterVersion.newBuilder()
        .setVersion(RuntimeConstants.VERSION).setState("Standby").setAddresses(
            NetAddress.newBuilder().setHost("hostname3").setRpcPort(10002).build()
        ).build();
    mMasterInfo = MasterInfo.newBuilder(mMasterInfo)
        .addAllZookeeperAddresses(Arrays.asList("[zookeeper_hostname1]:2181",
            "[zookeeper_hostname2]:2181", "[zookeeper_hostname3]:2181"))
        .addAllMasterVersions(Arrays.asList(primaryVersion, standby1Version, standby2Version))
        .setRaftJournal(false)
        .build();
    when(mMetaMasterClient.getMasterInfo(any())).thenReturn(mMasterInfo);
    SummaryCommand summaryCommand = new SummaryCommand(mMetaMasterClient,
        mBlockMasterClient, sConf.getString(PropertyKey.USER_DATE_FORMAT_PATTERN), mPrintStream);
    summaryCommand.run();
    checkIfOutputValid(sConf.getString(PropertyKey.USER_DATE_FORMAT_PATTERN), "zk");
  }

  @Test
  public void RaftHaSummary() throws IOException {
    MasterVersion primaryVersion = MasterVersion.newBuilder()
        .setVersion(RuntimeConstants.VERSION).setState("Primary").setAddresses(
            NetAddress.newBuilder().setHost("hostname1").setRpcPort(10000).build()
        ).build();
    MasterVersion standby1Version = MasterVersion.newBuilder()
        .setVersion(RuntimeConstants.VERSION).setState("Standby").setAddresses(
            NetAddress.newBuilder().setHost("hostname2").setRpcPort(10001).build()
        ).build();
    MasterVersion standby2Version = MasterVersion.newBuilder()
        .setVersion(RuntimeConstants.VERSION).setState("Standby").setAddresses(
            NetAddress.newBuilder().setHost("hostname3").setRpcPort(10002).build()
        ).build();
    mMasterInfo = MasterInfo.newBuilder(mMasterInfo)
        .setRaftJournal(true)
        .addAllRaftAddress(Arrays.asList("[raftJournal_hostname1]:19200",
            "[raftJournal_hostname2]:19200", "[raftJournal_hostname3]:19200"))
        .addAllMasterVersions(Arrays.asList(primaryVersion, standby1Version, standby2Version))
        .build();
    when(mMetaMasterClient.getMasterInfo(any())).thenReturn(mMasterInfo);
    SummaryCommand summaryCommand = new SummaryCommand(mMetaMasterClient,
        mBlockMasterClient, sConf.getString(PropertyKey.USER_DATE_FORMAT_PATTERN), mPrintStream);
    summaryCommand.run();
    checkIfOutputValid(sConf.getString(PropertyKey.USER_DATE_FORMAT_PATTERN), "raft");
  }

  /**
   * Checks if the output is expected.
   */
  private void checkIfOutputValid(String dateFormatPattern, String HAPattern)
      throws JsonProcessingException {
    String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree(output);

    // check master
    String versionStr = String.format("%s", RuntimeConstants.VERSION);
    assertEquals("testAddress", jsonNode.get("masterAddress").asText());
    assertEquals("10000", jsonNode.get("masterVersions").get(0).get("port").asText());
    assertEquals(versionStr, jsonNode.get("masterVersions").get(0).get("version").asText());
    assertEquals("hostname1", jsonNode.get("masterVersions").get(0).get("host").asText());
    assertEquals("Primary", jsonNode.get("masterVersions").get(0).get("state").asText());
    assertEquals("10001", jsonNode.get("masterVersions").get(1).get("port").asText());
    assertEquals(versionStr, jsonNode.get("masterVersions").get(1).get("version").asText());
    assertEquals("hostname2", jsonNode.get("masterVersions").get(1).get("host").asText());
    assertEquals("Standby", jsonNode.get("masterVersions").get(1).get("state").asText());
    assertEquals("10002", jsonNode.get("masterVersions").get(2).get("port").asText());
    assertEquals(versionStr, jsonNode.get("masterVersions").get(2).get("version").asText());
    assertEquals("hostname3", jsonNode.get("masterVersions").get(2).get("host").asText());
    assertEquals("Standby", jsonNode.get("masterVersions").get(2).get("state").asText());

    // check cluster summary
    // Skip checking startTime which relies on system time zone
    assertEquals("1231", jsonNode.get("webPort").asText());
    assertEquals("8462", jsonNode.get("rpcPort").asText());
    assertEquals("testVersion", jsonNode.get("version").asText());
    assertEquals("1131242343122", jsonNode.get("startTime").asText());
    assertEquals("12412412312", jsonNode.get("uptimeDuration").asText());
    assertEquals("false", jsonNode.get("safeMode").asText());

    // check zookeeper and raft
    if (Objects.equals(HAPattern, "zk")) {
      assertEquals("true", jsonNode.get("useZookeeper").asText());
      assertEquals("false", jsonNode.get("useRaftJournal").asText());
      assertEquals("[zookeeper_hostname1]:2181",
          jsonNode.get("zookeeperAddress").get(0).asText());
      assertEquals("[zookeeper_hostname2]:2181",
          jsonNode.get("zookeeperAddress").get(1).asText());
      assertEquals("[zookeeper_hostname3]:2181",
          jsonNode.get("zookeeperAddress").get(2).asText());
    } else if (Objects.equals(HAPattern, "raft")) {
      assertEquals("false", jsonNode.get("useZookeeper").asText());
      assertEquals("true", jsonNode.get("useRaftJournal").asText());
      assertEquals("[raftJournal_hostname1]:19200",
          jsonNode.get("raftJournalAddress").get(0).asText());
      assertEquals("[raftJournal_hostname2]:19200",
          jsonNode.get("raftJournalAddress").get(1).asText());
      assertEquals("[raftJournal_hostname3]:19200",
          jsonNode.get("raftJournalAddress").get(2).asText());
    } else {
      fail("HAPattern is neither zk nor raft");
    }

    // check worker
    assertEquals("12", jsonNode.get("liveWorkers").asText());
    assertEquals("4", jsonNode.get("lostWorkers").asText());
    assertEquals("1278919", jsonNode.get("freeCapacityBytes").asText());
    assertEquals("236501", jsonNode.get("totalCapacityOnTiers").get("DOMBytes").asText());
    assertEquals("1341353", jsonNode.get("totalCapacityOnTiers").get("MEMBytes").asText());
    assertEquals("23112", jsonNode.get("totalCapacityOnTiers").get("RAMBytes").asText());
    assertEquals("74235", jsonNode.get("usedCapacityOnTiers").get("DOMBytes").asText());
    assertEquals("62434", jsonNode.get("usedCapacityOnTiers").get("MEMBytes").asText());
    assertEquals("6243", jsonNode.get("usedCapacityOnTiers").get("RAMBytes").asText());
  }
}
