/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.LocalAlluxioClusterResource;
import alluxio.master.block.BlockMaster;
import alluxio.rest.TestCaseFactory;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerInfoTest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Test cases for {@link AlluxioMasterRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockMaster.class)
@Ignore
public class AlluxioMasterRestApiTest {
  private static final Map<String, String> NO_PARAMS = Maps.newHashMap();
  private static BlockMaster sBlockMaster;

  @Rule
  private LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource();

  @BeforeClass
  public static void beforeClass() {
    sBlockMaster = PowerMockito.mock(BlockMaster.class);
    AlluxioMaster alluxioMaster = PowerMockito.mock(AlluxioMaster.class);
    Mockito.doReturn(sBlockMaster).when(alluxioMaster).getBlockMaster();
    Whitebox.setInternalState(AlluxioMaster.class, "sAlluxioMaster", alluxioMaster);
  }

  private String getEndpoint(String suffix) {
    return AlluxioMasterRestServiceHandler.SERVICE_PREFIX + "/" + suffix;
  }

  @Test
  public void getCapacityBytesTest() throws Exception {
    Random random = new Random();
    long capacityBytes = random.nextLong();
    Mockito.doReturn(capacityBytes).when(sBlockMaster).getCapacityBytes();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_CAPACITY_BYTES),
            NO_PARAMS, "GET", capacityBytes, mResource).run();

    Mockito.verify(sBlockMaster).getCapacityBytes();
  }

  @Test
  public void getUsedBytesTest() throws Exception {
    Random random = new Random();
    long usedBytes = random.nextLong();
    Mockito.doReturn(usedBytes).when(sBlockMaster).getUsedBytes();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_USED_BYTES),
            NO_PARAMS, "GET", usedBytes, mResource).run();

    Mockito.verify(sBlockMaster).getUsedBytes();
  }

  @Test
  public void getWorkerInfoListTest() throws Exception {
    Random random = new Random();
    List<WorkerInfo> workerInfos = Lists.newArrayList();
    int numWorkerInfos = random.nextInt(10);
    for (int i = 0; i < numWorkerInfos; i++) {
      workerInfos.add(WorkerInfoTest.createRandom());
    }
    Mockito.doReturn(workerInfos).when(sBlockMaster).getWorkerInfoList();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_WORKER_INFO_LIST),
            NO_PARAMS, "GET", workerInfos, mResource).run();

    Mockito.verify(sBlockMaster).getWorkerInfoList();
  }
}
