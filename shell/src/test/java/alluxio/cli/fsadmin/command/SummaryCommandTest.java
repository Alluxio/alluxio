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

import alluxio.cli.fsadmin.report.SummaryCommand;
import alluxio.client.MetaMasterClient;
import alluxio.client.block.BlockMasterClient;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.BlockMasterInfo;
import alluxio.wire.MasterInfo;

import com.google.common.base.Strings;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class SummaryCommandTest {
  private static final int INDENT_SIZE = 4;
  private static final String ADDRESS = "testAddress";
  private static final int WEB_PORT = 1231;
  private static final int RPC_PORT = 8462;
  private static final long START_TIME_MS = 213124234312231L;
  private static final long UPTIME_MS = 12412412312L;
  private static final String VERSION = "testVersion";
  private static final boolean SAFE_MODE = false;
  private static final int LIVE_WORKER_NUM = 12;
  private static final int LOST_WORKER_NUM = 4;
  private static final long CAPACITY_BYTES = 1341353L;
  private static final long USED_BYTES = 62434L;
  private static final long FREE_BYTES = 1278919L;

  private MetaMasterClient mMetaMasterClient;
  private BlockMasterClient mBlockMasterClient;
  private ByteArrayOutputStream mOutputStream;
  private PrintStream mPrintStream;
  private int mIndentationLevel = 1;
  private Map<String, Long> mCapacityBytesOnTiers = new HashMap<>();
  private Map<String, Long> mUsedBytesOnTiers = new HashMap<>();

  @Before
  public void prepareDependencies() throws IOException {
    // Prepare mock meta master client
    mMetaMasterClient = Mockito.mock(MetaMasterClient.class);
    MasterInfo masterInfo = new MasterInfo()
        .setMasterAddress(ADDRESS)
        .setWebPort(WEB_PORT)
        .setRpcPort(RPC_PORT)
        .setStartTimeMs(START_TIME_MS)
        .setUpTimeMs(UPTIME_MS)
        .setVersion(VERSION)
        .setSafeMode(SAFE_MODE);
    Mockito.when(mMetaMasterClient.getMasterInfo(Mockito.any())).thenReturn(masterInfo);

    // Prepare mock block master client
    mBlockMasterClient = Mockito.mock(BlockMasterClient.class);
    mCapacityBytesOnTiers.put("MEM", CAPACITY_BYTES);
    mUsedBytesOnTiers.put("MEM", USED_BYTES);
    BlockMasterInfo blockMasterInfo = new BlockMasterInfo()
        .setLiveWorkerNum(LIVE_WORKER_NUM)
        .setLostWorkerNum(LOST_WORKER_NUM)
        .setCapacityBytes(CAPACITY_BYTES)
        .setCapacityBytesOnTiers(mCapacityBytesOnTiers)
        .setUsedBytes(USED_BYTES)
        .setUsedBytesOnTiers(mUsedBytesOnTiers)
        .setFreeBytes(FREE_BYTES);
    Mockito.when(mBlockMasterClient.getBlockMasterInfo(Mockito.any()))
        .thenReturn(blockMasterInfo);

    // Prepare print stream
    mOutputStream = new ByteArrayOutputStream();
    mPrintStream = new PrintStream(mOutputStream, true, "utf-8");
  }

  @After
  public void cleanDependencies() throws IOException {
    mMetaMasterClient.close();
    mBlockMasterClient.close();
    mPrintStream.close();
  }

  @Test
  public void callSummaryWithDependencies() throws IOException {
    SummaryCommand summaryCommand = new SummaryCommand(mMetaMasterClient,
        mBlockMasterClient, mPrintStream);
    summaryCommand.run();
    checkIfOutputValid();
  }

  /**
   * Checks if the output is expected.
   */
  private void checkIfOutputValid() throws IOException {
    String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);
    String[] outputLines = output.split("\n");
    for (int lineNum = 0; lineNum < outputLines.length; lineNum++) {
      switch (lineNum) {
        case 0:
          Assert.assertEquals("Alluxio Cluster Summary: ", outputLines[lineNum]);
          break;
        case 1:
          Assert.assertEquals(indent("Master Address: " + ADDRESS), outputLines[lineNum]);
          break;
        case 2:
          Assert.assertEquals(indent("Web Port: " + WEB_PORT), outputLines[lineNum]);
          break;
        case 3:
          Assert.assertEquals(indent("Rpc Port: " + RPC_PORT), outputLines[lineNum]);
          break;
        case 4:
          Assert.assertEquals(indent("Started: "
              + CommonUtils.convertMsToDate(START_TIME_MS)), outputLines[lineNum]);
          break;
        case 5:
          Assert.assertEquals(indent("Uptime: "
              + CommonUtils.convertMsToClockTime(UPTIME_MS)), outputLines[lineNum]);
          break;
        case 6:
          Assert.assertEquals(indent("Version: " + VERSION), outputLines[lineNum]);
          break;
        case 7:
          Assert.assertEquals(indent("Safe Mode: " + SAFE_MODE), outputLines[lineNum]);
          break;
        case 8:
          Assert.assertEquals(indent("Live Workers: " + LIVE_WORKER_NUM), outputLines[lineNum]);
          break;
        case 9:
          Assert.assertEquals(indent("Lost Workers: " + LOST_WORKER_NUM), outputLines[lineNum]);
          break;
        case 10:
          Assert.assertEquals(indent("Total Capacity: "
              + FormatUtils.getSizeFromBytes(CAPACITY_BYTES)), outputLines[lineNum]);
          break;
        case 11:
          mIndentationLevel++;
          Assert.assertEquals(indent("Tier: MEM  Size: "
              + FormatUtils.getSizeFromBytes(CAPACITY_BYTES)), outputLines[lineNum]);
          break;
        case 12:
          mIndentationLevel--;
          Assert.assertEquals(indent("Used Capacity: "
              + FormatUtils.getSizeFromBytes(USED_BYTES)), outputLines[lineNum]);
          break;
        case 13:
          mIndentationLevel++;
          Assert.assertEquals(indent("Tier: MEM  Size: "
              + FormatUtils.getSizeFromBytes(USED_BYTES)), outputLines[lineNum]);
          break;
        case 14:
          mIndentationLevel--;
          Assert.assertEquals(indent("Free Capacity: "
              + FormatUtils.getSizeFromBytes(FREE_BYTES)), outputLines[lineNum]);
          break;
        default:
          break;
      }
    }
  }

  /**
   * Converts original string to indented string.
   *
   * @param text information to convert
   */
  private String indent(String text) {
    String indent = Strings.repeat(" ", mIndentationLevel * INDENT_SIZE);
    return indent + text;
  }
}
