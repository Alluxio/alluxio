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
import alluxio.Configuration;
import alluxio.ProjectConstants;
import alluxio.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.BlockMasterInfo;
import alluxio.wire.BlockMasterInfo.BlockMasterInfoField;
import alluxio.wire.MasterInfo;
import alluxio.wire.MasterInfo.MasterInfoField;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SummaryCommandTest {
  private MetaMasterClient mMetaMasterClient;
  private String mAddress;
  private int mWebPort;
  private int mRpcPort;
  private long mStartTimeMs;
  private long mUpTimeMs;
  private String mVersion;
  private boolean mSafeMode;

  private BlockMasterClient mBlockMasterClient;
  private int mLiveWorkerNum;
  private int mLostWorkerNum;
  private long mCapacityBytes;
  private Map<String, Long> mCapacityBytesOnTiers;
  private long mUsedBytes;
  private Map<String, Long> mUsedBytesOnTiers;
  private long mFreeBytes;

  private File mFile;
  private PrintStream mPrintStream;

  @Before
  public void prepareDependencies() throws IOException {
    setInfoValues();

    // Prepare mock meta master client
    mMetaMasterClient = Mockito.mock(MetaMasterClient.class);
    MasterInfo masterInfo = new MasterInfo().setMasterAddress(mAddress)
        .setWebPort(mWebPort).setRpcPort(mRpcPort).setStartTimeMs(mStartTimeMs)
        .setUpTimeMs(mUpTimeMs).setVersion(mVersion).setSafeMode(mSafeMode);
    Set<MasterInfoField> masterInfoFilter = new HashSet<>(Arrays
        .asList(MasterInfoField.MASTER_ADDRESS, MasterInfoField.WEB_PORT,
            MasterInfoField.RPC_PORT, MasterInfoField.START_TIME_MS,
            MasterInfoField.UP_TIME_MS, MasterInfoField.VERSION,
            MasterInfoField.SAFE_MODE));
    Mockito.when(mMetaMasterClient.getMasterInfo(masterInfoFilter)).thenReturn(masterInfo);

    // Prepare mock block master client
    mBlockMasterClient = Mockito.mock(BlockMasterClient.class);
    BlockMasterInfo blockMasterInfo = new BlockMasterInfo()
        .setLiveWorkerNum(mLiveWorkerNum).setLostWorkerNum(mLostWorkerNum)
        .setCapacityBytes(mCapacityBytes).setCapacityBytesOnTiers(mCapacityBytesOnTiers)
        .setUsedBytes(mUsedBytes).setUsedBytesOnTiers(mUsedBytesOnTiers)
        .setFreeBytes(mFreeBytes);
    Set<BlockMasterInfoField> blockMasterInfoFilter = new HashSet<>(Arrays
        .asList(BlockMasterInfoField.LIVE_WORKER_NUM, BlockMasterInfoField.LOST_WORKER_NUM,
            BlockMasterInfoField.CAPACITY_BYTES, BlockMasterInfoField.CAPACITY_BYTES_ON_TIERS,
            BlockMasterInfoField.USED_BYTES, BlockMasterInfoField.USED_BYTES_ON_TIERS,
            BlockMasterInfoField.FREE_BYTES));
    Mockito.when(mBlockMasterClient.getBlockMasterInfo(blockMasterInfoFilter))
        .thenReturn(blockMasterInfo);

    // Prepare print stream
    mFile = new File("./MockSummaryResult.txt");
    FileOutputStream fileOutputStream = new FileOutputStream(mFile);
    mPrintStream = new PrintStream(fileOutputStream);
  }

  @After
  public void cleanDependencies() throws IOException {
    mMetaMasterClient.close();
    mBlockMasterClient.close();
    mPrintStream.close();
    mFile.delete();
  }

  @Test
  public void callSummaryWithDependencies() throws IOException {
    SummaryCommand summaryCommand = new SummaryCommand(mMetaMasterClient,
        mBlockMasterClient, mPrintStream);
    summaryCommand.run();
    mPrintStream.flush();

    BufferedReader reader = new BufferedReader(new FileReader(mFile));
    checkIfOutputValid(reader);
  }

  /**
   * Checks if the output contains expected values.
   *
   * @param reader reader to read SummaryCommand output
   */
  private void checkIfOutputValid(BufferedReader reader) throws IOException {
    StringBuilder outputBuilder = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      outputBuilder.append(line);
    }
    String output = outputBuilder.toString();

    Assert.assertThat(output, CoreMatchers.containsString(
        "Master Address: " + mAddress));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Web Port: " + mWebPort));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Rpc Port: " + mRpcPort));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Started: " + CommonUtils.convertMsToDate(mStartTimeMs)));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Uptime: " + CommonUtils.convertMsToClockTime(mUpTimeMs)));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Version: " + mVersion));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Safe Mode: " + mSafeMode));

    Assert.assertThat(output, CoreMatchers.containsString("Live Workers: " + mLiveWorkerNum));
    Assert.assertThat(output, CoreMatchers.containsString("Lost Workers: " + mLostWorkerNum));
    Assert.assertThat(output, CoreMatchers.containsString("Total Capacity: "
        + FormatUtils.getSizeFromBytes(mCapacityBytes)));
    Assert.assertThat(output, CoreMatchers.containsString("Tier: MEM"
        + "  Size: " + FormatUtils.getSizeFromBytes(mCapacityBytesOnTiers.get("MEM"))));
    Assert.assertThat(output, CoreMatchers.containsString("Used Capacity: "
        + FormatUtils.getSizeFromBytes(mUsedBytes)));
    Assert.assertThat(output, CoreMatchers.containsString("Tier: MEM"
        + "  Size: " + FormatUtils.getSizeFromBytes(mUsedBytesOnTiers.get("MEM"))));
    Assert.assertThat(output, CoreMatchers.containsString("Free Capacity: "
        + FormatUtils.getSizeFromBytes(mFreeBytes)));
  }

  /**
   * Sets the client info values so that we can do explicitly check.
   */
  private void setInfoValues() {
    // Set meta master info values
    mAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC).toString();
    mWebPort = Integer.valueOf(Configuration.get(PropertyKey.MASTER_WEB_PORT));
    mRpcPort = Integer.valueOf(Configuration.get(PropertyKey.MASTER_RPC_PORT));
    mStartTimeMs = System.currentTimeMillis();
    mUpTimeMs = 60000;
    mVersion = ProjectConstants.VERSION;
    mSafeMode = false;

    // Set block master info values
    mLiveWorkerNum = 1;
    mLostWorkerNum = 0;
    mCapacityBytes = 1000000L;
    mUsedBytes = 500000L;
    mFreeBytes = 500000L;
    mCapacityBytesOnTiers = new HashMap<>();
    mCapacityBytesOnTiers.put("MEM", 1000000L);
    mUsedBytesOnTiers = new HashMap<>();
    mUsedBytesOnTiers.put("MEM", 500000L);
  }
}
