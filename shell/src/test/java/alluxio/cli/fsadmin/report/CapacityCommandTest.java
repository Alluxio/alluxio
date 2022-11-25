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

import alluxio.Constants;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CapacityCommandTest {
  private BlockMasterClient mBlockMasterClient;

  @Before
  public void prepareDependencies() throws IOException {
    // Prepare mock block master client
    mBlockMasterClient = Mockito.mock(BlockMasterClient.class);
  }

  @Test
  public void longCapacity() throws IOException {
    List<WorkerInfo> longInfoList = prepareLongInfoList();
    Mockito.when(mBlockMasterClient.getWorkerReport(Mockito.any()))
        .thenReturn(longInfoList);

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
         PrintStream printStream = new PrintStream(outputStream, true, "utf-8")) {
      CapacityCommand capacityCommand = new CapacityCommand(mBlockMasterClient,
          printStream);
      capacityCommand.generateCapacityReport(GetWorkerReportOptions.defaults());
      String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
      // CHECKSTYLE.OFF: LineLengthExceed - Much more readable
      List<String> expectedOutput = Arrays.asList("Capacity information for all workers: ",
          "    Total Capacity: 29.80GB",
          "        Tier: MEM  Size: 8.38GB",
          "        Tier: SSD  Size: 4768.37MB",
          "        Tier: HDD  Size: 1907.35MB",
          "        Tier: DOM  Size: 9.31GB",
          "        Tier: RAM  Size: 5.59GB",
          "    Used Capacity: 10.24GB",
          "        Tier: MEM  Size: 3051.76MB",
          "        Tier: SSD  Size: 286.10MB",
          "        Tier: HDD  Size: 1907.35MB",
          "        Tier: DOM  Size: 476.84MB",
          "        Tier: RAM  Size: 4768.37MB",
          "    Used Percentage: 34%",
          "    Free Percentage: 66%",
          "",
          "Worker Name      Last Heartbeat   Storage       Total            MEM           SSD           HDD           DOM           RAM           ",
          "216.239.33.96    542              capacity      18.63GB          4768.37MB     4768.37MB     -             9.31GB        -             ",
          "                                  used          953.67MB (5%)    190.73MB      286.10MB      -             476.84MB      -             ",
          "64.68.90.1       3123             capacity      11.18GB          3814.70MB     -             1907.35MB     -             5.59GB        ",
          "                                  used          9.31GB (83%)     2861.02MB     -             1907.35MB     -             4768.37MB     ");
      // CHECKSTYLE.ON: LineLengthExceed
      List<String> testOutput = Arrays.asList(output.split("\n"));
      Assert.assertThat(testOutput,
          IsIterableContainingInOrder.contains(expectedOutput.toArray()));
    }
  }

  @Test
  public void shortCapacity() throws IOException {
    List<WorkerInfo> shortInfoList = prepareShortInfoList();
    Mockito.when(mBlockMasterClient.getWorkerReport(Mockito.any()))
        .thenReturn(shortInfoList);

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
         PrintStream printStream = new PrintStream(outputStream, true, "utf-8")) {
      CapacityCommand capacityCommand = new CapacityCommand(mBlockMasterClient,
          printStream);
      capacityCommand.generateCapacityReport(GetWorkerReportOptions.defaults());
      String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
      List<String> expectedOutput = Arrays.asList("Capacity information for all workers: ",
          "    Total Capacity: 14.90GB",
          "        Tier: RAM  Size: 14.90GB",
          "    Used Capacity: 5.12GB",
          "        Tier: RAM  Size: 5.12GB",
          "    Used Percentage: 34%",
          "    Free Percentage: 66%",
          "",
          "Worker Name      Last Heartbeat   Storage       RAM",
          "215.42.95.24     953              capacity      9.31GB",
          "                                  used          476.84MB (5%)",
          "29.53.5.124      6424122          capacity      5.59GB",
          "                                  used          4768.37MB (83%)");
      List<String> testOutput = Arrays.asList(output.split("\n"));
      Assert.assertThat(testOutput,
          IsIterableContainingInOrder.contains(expectedOutput.toArray()));
    }
  }

  @Test
  public void longWorkerNameCapacity() throws IOException {
    List<WorkerInfo> longWorkerNameInfoList = prepareLongWorkerNameInfoList();
    Mockito.when(mBlockMasterClient.getWorkerReport(Mockito.any()))
        .thenReturn(longWorkerNameInfoList);

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
         PrintStream printStream = new PrintStream(outputStream, true, "utf-8")) {
      CapacityCommand capacityCommand = new CapacityCommand(mBlockMasterClient,
          printStream);
      capacityCommand.generateCapacityReport(GetWorkerReportOptions.defaults());
      String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
      List<String> testRst = Arrays.asList(output.split("\n"));
      // CHECKSTYLE.OFF: LineLengthExceed - Much more readable
      List<String> expectedOutput = Arrays.asList("Capacity information for all workers: ",
          "    Total Capacity: 3051.76MB",
          "        Tier: MEM  Size: 1144.41MB",
          "        Tier: SSD  Size: 572.20MB",
          "        Tier: HDD  Size: 190.73MB",
          "    Used Capacity: 1049.04MB",
          "        Tier: MEM  Size: 305.18MB",
          "        Tier: SSD  Size: 28.61MB",
          "        Tier: HDD  Size: 190.73MB",
          "    Used Percentage: 34%",
          "    Free Percentage: 66%",
          "",
          "Worker Name                 Last Heartbeat   Storage       Total            MEM           SSD           HDD           ",
          "org.apache.hdp1             681              capacity      1907.35MB        572.20MB      572.20MB      -             ",
          "                                             used          95.37MB (5%)     19.07MB       28.61MB       -             ",
          "org.alluxio.long.host1      6211             capacity      1144.41MB        572.20MB      -             190.73MB      ",
          "                                             used          953.67MB (83%)   286.10MB      -             190.73MB      ");
      // CHECKSTYLE.ON: LineLengthExceed
      List<String> testOutput = Arrays.asList(output.split("\n"));

      Assert.assertThat(testOutput,
          IsIterableContainingInOrder.contains(expectedOutput.toArray()));
    }
  }

  /**
   * @return long worker info list to test
   */
  private List<WorkerInfo> prepareLongInfoList() {
    List<WorkerInfo> infoList = new ArrayList<>();
    Map<String, Long> capacityBytesOnTiersOne = new HashMap<>();
    capacityBytesOnTiersOne.put(Constants.MEDIUM_MEM, 4000000000L);
    capacityBytesOnTiersOne.put("RAM", 6000000000L);
    capacityBytesOnTiersOne.put(Constants.MEDIUM_HDD, 2000000000L);
    Map<String, Long> usedBytesOnTiersOne = new HashMap<>();
    usedBytesOnTiersOne.put(Constants.MEDIUM_MEM, 3000000000L);
    usedBytesOnTiersOne.put("RAM", 5000000000L);
    usedBytesOnTiersOne.put(Constants.MEDIUM_HDD, 2000000000L);
    WorkerInfo firstInfo = new WorkerInfo()
        .setAddress(new WorkerNetAddress().setHost("64.68.90.1"))
        .setCapacityBytes(12000000000L)
        .setCapacityBytesOnTiers(capacityBytesOnTiersOne)
        .setId(1)
        .setLastContactSec(3123)
        .setStartTimeMs(1331231121212L)
        .setState("In Service")
        .setUsedBytes(10000000000L)
        .setUsedBytesOnTiers(usedBytesOnTiersOne);

    Map<String, Long> capacityBytesOnTiersSec = new HashMap<>();
    capacityBytesOnTiersSec.put(Constants.MEDIUM_MEM, 5000000000L);
    capacityBytesOnTiersSec.put(Constants.MEDIUM_SSD, 5000000000L);
    capacityBytesOnTiersSec.put("DOM", 10000000000L);
    Map<String, Long> usedBytesOnTiersSec = new HashMap<>();
    usedBytesOnTiersSec.put(Constants.MEDIUM_MEM, 200000000L);
    usedBytesOnTiersSec.put(Constants.MEDIUM_SSD, 300000000L);
    usedBytesOnTiersSec.put("DOM", 500000000L);
    WorkerInfo secondInfo = new WorkerInfo()
        .setAddress(new WorkerNetAddress().setHost("216.239.33.96"))
        .setCapacityBytes(20000000000L)
        .setCapacityBytesOnTiers(capacityBytesOnTiersSec)
        .setId(2)
        .setLastContactSec(542)
        .setStartTimeMs(1131231121212L)
        .setState("In Service")
        .setUsedBytes(1000000000L)
        .setUsedBytesOnTiers(usedBytesOnTiersSec);

    infoList.add(firstInfo);
    infoList.add(secondInfo);
    return infoList;
  }

  /**
   * @return short worker info list that only one tier exists
   */
  private List<WorkerInfo> prepareShortInfoList() {
    List<WorkerInfo> infoList = new ArrayList<>();
    Map<String, Long> capacityBytesOnTiersOne = new HashMap<>();
    capacityBytesOnTiersOne.put("RAM", 6000000000L);
    Map<String, Long> usedBytesOnTiersOne = new HashMap<>();
    usedBytesOnTiersOne.put("RAM", 5000000000L);
    WorkerInfo firstInfo = new WorkerInfo()
        .setAddress(new WorkerNetAddress().setHost("29.53.5.124"))
        .setCapacityBytes(6000000000L)
        .setCapacityBytesOnTiers(capacityBytesOnTiersOne)
        .setId(1)
        .setLastContactSec(6424122)
        .setStartTimeMs(19365332L)
        .setState("Out of Service")
        .setUsedBytes(5000000000L)
        .setUsedBytesOnTiers(usedBytesOnTiersOne);

    Map<String, Long> capacityBytesOnTiersSec = new HashMap<>();
    capacityBytesOnTiersSec.put("RAM", 10000000000L);
    Map<String, Long> usedBytesOnTiersSec = new HashMap<>();
    usedBytesOnTiersSec.put("RAM", 500000000L);
    WorkerInfo secondInfo = new WorkerInfo()
        .setAddress(new WorkerNetAddress().setHost("215.42.95.24"))
        .setCapacityBytes(10000000000L)
        .setCapacityBytesOnTiers(capacityBytesOnTiersSec)
        .setId(2)
        .setLastContactSec(953)
        .setStartTimeMs(112495222L)
        .setState("In Service")
        .setUsedBytes(500000000L)
        .setUsedBytesOnTiers(usedBytesOnTiersSec);

    infoList.add(firstInfo);
    infoList.add(secondInfo);
    return infoList;
  }

  /**
   * @return long worker name info list to test
   */
  private List<WorkerInfo> prepareLongWorkerNameInfoList() {
    List<WorkerInfo> infoList = new ArrayList<>();
    Map<String, Long> capacityBytesOnTiersOne = new HashMap<>();
    capacityBytesOnTiersOne.put(Constants.MEDIUM_MEM, 600000000L);
    capacityBytesOnTiersOne.put(Constants.MEDIUM_HDD, 200000000L);
    Map<String, Long> usedBytesOnTiersOne = new HashMap<>();
    usedBytesOnTiersOne.put(Constants.MEDIUM_MEM, 300000000L);
    usedBytesOnTiersOne.put(Constants.MEDIUM_HDD, 200000000L);
    WorkerInfo firstInfo = new WorkerInfo()
        .setAddress(new WorkerNetAddress().setHost("org.alluxio.long.host1"))
        .setCapacityBytes(1200000000L)
        .setCapacityBytesOnTiers(capacityBytesOnTiersOne)
        .setId(1)
        .setLastContactSec(6211)
        .setStartTimeMs(1529222699127L)
        .setState("In Service")
        .setUsedBytes(1000000000L)
        .setUsedBytesOnTiers(usedBytesOnTiersOne);

    Map<String, Long> capacityBytesOnTiersSec = new HashMap<>();
    capacityBytesOnTiersSec.put(Constants.MEDIUM_MEM, 600000000L);
    capacityBytesOnTiersSec.put(Constants.MEDIUM_SSD, 600000000L);
    Map<String, Long> usedBytesOnTiersSec = new HashMap<>();
    usedBytesOnTiersSec.put(Constants.MEDIUM_MEM, 20000000L);
    usedBytesOnTiersSec.put(Constants.MEDIUM_SSD, 30000000L);
    WorkerInfo secondInfo = new WorkerInfo()
        .setAddress(new WorkerNetAddress().setHost("org.apache.hdp1"))
        .setCapacityBytes(2000000000L)
        .setCapacityBytesOnTiers(capacityBytesOnTiersSec)
        .setId(2)
        .setLastContactSec(681)
        .setStartTimeMs(1529222699127L)
        .setState("In Service")
        .setUsedBytes(100000000L)
        .setUsedBytesOnTiers(usedBytesOnTiersSec);

    infoList.add(firstInfo);
    infoList.add(secondInfo);
    return infoList;
  }
}
