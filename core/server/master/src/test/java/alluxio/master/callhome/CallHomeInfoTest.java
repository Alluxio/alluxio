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

package alluxio.master.callhome;

import alluxio.RuntimeConstants;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerInfoTest;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link CallHomeInfo}.
 */
public final class CallHomeInfoTest {

  WorkerInfo[] mWorkerInfos;
  WorkerInfo[] mLostWorkerInfos;

  @Before
  public void before() throws Exception {
    mWorkerInfos = getTestWorkerInfoList(3);
    mLostWorkerInfos = getTestWorkerInfoList(1);
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() throws Exception {
    CallHomeInfo info = getTestDiagnosticInfo();

    Assert.assertFalse(info.getFaultTolerant());
    Assert.assertEquals(1111, info.getStartTime());
    Assert.assertEquals(11111, info.getUptime());
    Assert.assertEquals(RuntimeConstants.VERSION, info.getClusterVersion());

    Assert.assertEquals(4, info.getWorkerCount());
    Assert.assertTrue(Arrays.equals(mWorkerInfos, info.getWorkerInfos()));
    Assert.assertEquals(1, info.getLostWorkerCount());
    Assert.assertTrue(Arrays.equals(mLostWorkerInfos, info.getLostWorkerInfos()));

    Assert.assertTrue("local".equals(info.getUfsType()));
    Assert.assertEquals(22222, info.getUfsSize());

    Assert.assertTrue(Arrays.equals(getTestStorageTiers(), info.getStorageTiers()));

    Assert.assertTrue("masterhost/127.0.0.1:19998".equals(info.getMasterAddress()));
    Assert.assertEquals(100, info.getNumberOfPaths());
  }

  /**
   * @return test instance of {@link CallHomeInfo}
   */
  public CallHomeInfo getTestDiagnosticInfo() {
    CallHomeInfo info = new CallHomeInfo();

    info.setFaultTolerant(false);
    info.setStartTime(1111);
    info.setUptime(11111);
    info.setClusterVersion(RuntimeConstants.VERSION);
    // Set worker info
    info.setWorkerCount(4);
    info.setWorkerInfos(mWorkerInfos);
    info.setLostWorkerCount(1);
    info.setLostWorkerInfos(mLostWorkerInfos);
    // Set ufs information
    info.setUfsType("local");
    info.setUfsSize(22222);
    // Set storage tiers
    info.setStorageTiers(getTestStorageTiers());
    // Set file system master info
    info.setMasterAddress("masterhost/127.0.0.1:19998");
    info.setNumberOfPaths(100);
    return info;
  }

  private static CallHomeInfo.StorageTier[] getTestStorageTiers() {
    List<CallHomeInfo.StorageTier> tiers = Lists.newArrayList();
    CallHomeInfo.StorageTier tier = new CallHomeInfo.StorageTier();
    tier.setAlias("MEM");
    tier.setSize(100);
    tier.setUsedSizeInBytes(50);
    tiers.add(tier);
    return tiers.toArray(new CallHomeInfo.StorageTier[tiers.size()]);
  }

  private static WorkerInfo[] getTestWorkerInfoList(int count) {
    WorkerInfo[] workerInfos = new WorkerInfo[count];
    for (int i = 0; i < count; ++i) {
      workerInfos[i] = WorkerInfoTest.createRandom();
    }
    return workerInfos;
  }
}
