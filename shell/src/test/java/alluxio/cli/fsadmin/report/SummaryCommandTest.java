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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.meta.MetaMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.MasterInfo;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.BlockMasterInfo;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SummaryCommandTest {

  private static AlluxioConfiguration sConf =
      new InstancedConfiguration(ConfigurationUtils.copyDefaults());

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

  void prepareZKHADependencies() throws IOException {
    mMasterInfo = MasterInfo.newBuilder(mMasterInfo)
        .addAllZookeeperAddresses(Arrays.asList("[zookeeper_hostname1]:2181",
            "[zookeeper_hostname2]:2181", "[zookeeper_hostname3]:2181"))
        .setRaftJournal(false)
        .build();
    when(mMetaMasterClient.getMasterInfo(any())).thenReturn(mMasterInfo);
  }

  void prepareRaftHaDependencies() throws IOException {
    mMasterInfo = MasterInfo.newBuilder(mMasterInfo)
        .setRaftJournal(true)
        .addAllRaftAddress(Arrays.asList("[raftJournal_hostname1]:19200",
            "[raftJournal_hostname2]:19200", "[raftJournal_hostname3]:19200"))
        .build();
    when(mMetaMasterClient.getMasterInfo(any())).thenReturn(mMasterInfo);
  }

  @After
  public void after() {
    mPrintStream.close();
  }

  @Test
  public void ZkHaSummary() throws IOException {
    prepareZKHADependencies();
    SummaryCommand summaryCommand = new SummaryCommand(mMetaMasterClient,
        mBlockMasterClient, sConf.getString(PropertyKey.USER_DATE_FORMAT_PATTERN), mPrintStream);
    ArrayList<String> zkHAPattern = new ArrayList<>(Arrays.asList(
        "    Zookeeper Enabled: true",
        "    Zookeeper Addresses: ",
        "        [zookeeper_hostname1]:2181",
        "        [zookeeper_hostname2]:2181",
        "        [zookeeper_hostname3]:2181",
        "    Raft-based Journal: false"));
    summaryCommand.run();
    checkIfOutputValid(sConf.getString(PropertyKey.USER_DATE_FORMAT_PATTERN), zkHAPattern);
  }

  @Test
  public void RaftHaSummary() throws IOException {
    prepareRaftHaDependencies();
    SummaryCommand summaryCommand = new SummaryCommand(mMetaMasterClient,
        mBlockMasterClient, sConf.getString(PropertyKey.USER_DATE_FORMAT_PATTERN), mPrintStream);
    ArrayList<String> raftHaPattern = new ArrayList<>(Arrays.asList(
        "    Zookeeper Enabled: false",
        "    Raft-based Journal: true",
        "    Raft Journal Addresses: ",
        "        [raftJournal_hostname1]:19200",
        "        [raftJournal_hostname2]:19200",
        "        [raftJournal_hostname3]:19200"));
    summaryCommand.run();

    checkIfOutputValid(sConf.getString(PropertyKey.USER_DATE_FORMAT_PATTERN), raftHaPattern);
  }

  /**
   * Checks if the output is expected.
   */
  private void checkIfOutputValid(String dateFormatPattern, List<? extends String> HAPattern) {
    String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);
    // Skip checking startTime which relies on system time zone
    String startTime =  CommonUtils.convertMsToDate(1131242343122L, dateFormatPattern);
    List<String> expectedOutput = new ArrayList<>(Arrays.asList("Alluxio cluster summary: ",
        "    Master Address: testAddress",
        "    Web Port: 1231",
        "    Rpc Port: 8462",
        "    Started: " + startTime,
        "    Uptime: 143 day(s), 15 hour(s), 53 minute(s), and 32 second(s)",
        "    Version: testVersion",
        "    Safe Mode: false"));
    expectedOutput.addAll(HAPattern);
    expectedOutput.addAll(new ArrayList<>(Arrays.asList(
        "    Live Workers: 12",
        "    Lost Workers: 4",
        "    Total Capacity: 1309.92KB",
        "        Tier: MEM  Size: 1309.92KB",
        "        Tier: DOM  Size: 230.96KB",
        "        Tier: RAM  Size: 22.57KB",
        "    Used Capacity: 60.97KB",
        "        Tier: MEM  Size: 60.97KB",
        "        Tier: DOM  Size: 72.50KB",
        "        Tier: RAM  Size: 6.10KB",
        "    Free Capacity: 1248.94KB")));
    List<String> testOutput = Arrays.asList(output.split("\n"));
    Assert.assertThat(testOutput, IsIterableContainingInOrder.contains(expectedOutput.toArray()));
  }
}
