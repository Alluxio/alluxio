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

package alluxio.cli.fsadmin.command;

import alluxio.cli.fsadmin.report.CapacityCommand;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.options.WorkerInfoOptions;
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
    List<WorkerInfo> infoList = prepareInfoList();
    Mockito.when(mBlockMasterClient.getWorkerInfoList(Mockito.any()))
        .thenReturn(infoList);
  }

  @Test
  public void capacity() throws IOException {
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
         PrintStream printStream = new PrintStream(outputStream, true, "utf-8")) {
      CapacityCommand capacityCommand = new CapacityCommand(mBlockMasterClient,
          printStream);
      capacityCommand.generateCapacityReport(WorkerInfoOptions.defaults());
      String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
      List<String> expectedOutput = Arrays.asList("Capacity information for all workers: ",
          "    Total Capacity: 29.80GB",
          "        Tier: MEM  Size: 8.38GB",
          "        Tier: SSD  Size: 10.24GB",
          "        Tier: HDD  Size: 11.18GB",
          "    Used Capacity: 10.24GB",
          "        Tier: MEM  Size: 3051.76MB",
          "        Tier: SSD  Size: 5054.47MB",
          "        Tier: HDD  Size: 2384.19MB",
          "    Used Percentage: 34%",
          "    Free Percentage: 66%",
          "",
          "Worker Name      Last Heartbeat   Type          Total            "
              + "MEM           SSD           HDD          ",
          "64.68.90.1       3123             Capacity      11.18GB          "
              + "3814.70MB     5.59GB        1907.35MB    ",
          "                                  Used          9.31GB (83%)     "
              + "2861.02MB     4768.37MB     1907.35MB    ",
          "216.239.33.96    542              Capacity      18.63GB          "
              + "4768.37MB     4768.37MB     9.31GB       ",
          "                                  Used          953.67MB (5%)    "
              + "190.73MB      286.10MB      476.84MB     ");
      List<String> testOutput = Arrays.asList(output.split("\n"));
      Assert.assertThat(testOutput,
          IsIterableContainingInOrder.contains(expectedOutput.toArray()));
    }
  }

  /**
   * @return worker info list to test
   */
  private List<WorkerInfo> prepareInfoList() {
    List<WorkerInfo> infoList = new ArrayList<>();
    Map<String, Long> capacityBytesOnTiersOne = new HashMap<>();
    capacityBytesOnTiersOne.put("MEM", 4000000000L);
    capacityBytesOnTiersOne.put("SSD", 6000000000L);
    capacityBytesOnTiersOne.put("HDD", 2000000000L);
    Map<String, Long> usedBytesOnTiersOne = new HashMap<>();
    usedBytesOnTiersOne.put("MEM", 3000000000L);
    usedBytesOnTiersOne.put("SSD", 5000000000L);
    usedBytesOnTiersOne.put("HDD", 2000000000L);
    WorkerInfo firstInfo = new WorkerInfo()
        .setAddress(new WorkerNetAddress().setHost("64.68.90.1"))
        .setCapacityBytes(12000000000L)
        .setCapacityBytesOnTiers(capacityBytesOnTiersOne)
        .setId(1)
        .setLastContactSec(3123)
        .setStartTimeMs(1331231121212L)
        .setUsedBytes(10000000000L)
        .setUsedBytesOnTiers(usedBytesOnTiersOne);

    Map<String, Long> capacityBytesOnTiersSec = new HashMap<>();
    capacityBytesOnTiersSec.put("MEM", 5000000000L);
    capacityBytesOnTiersSec.put("SSD", 5000000000L);
    capacityBytesOnTiersSec.put("HDD", 10000000000L);
    Map<String, Long> usedBytesOnTiersSec = new HashMap<>();
    usedBytesOnTiersSec.put("MEM", 200000000L);
    usedBytesOnTiersSec.put("SSD", 300000000L);
    usedBytesOnTiersSec.put("HDD", 500000000L);
    WorkerInfo secondInfo = new WorkerInfo()
        .setAddress(new WorkerNetAddress().setHost("216.239.33.96"))
        .setCapacityBytes(20000000000L)
        .setCapacityBytesOnTiers(capacityBytesOnTiersSec)
        .setId(2)
        .setLastContactSec(542)
        .setStartTimeMs(1131231121212L)
        .setUsedBytes(1000000000L)
        .setUsedBytesOnTiers(usedBytesOnTiersSec);

    infoList.add(firstInfo);
    infoList.add(secondInfo);
    return infoList;
  }
}
