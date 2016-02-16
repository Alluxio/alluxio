/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.master.block;

import alluxio.LocalAlluxioClusterResource;
import alluxio.RestApiTestCaseFactory;
import alluxio.Constants;
import alluxio.master.AlluxioMaster;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockInfoTest;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerInfoTest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.BeforeClass;
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
 * Test cases for {@link BlockMasterClientRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockMaster.class)
public class BlockMasterClientRestApiTest {
  private static final Map<String, String> NO_PARAMS = Maps.<String, String>newHashMap();
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

  @Test
  public void serviceNameTest() throws Exception {
    RestApiTestCaseFactory
        .newMasterTestCase(BlockMasterClientRestServiceHandler.SERVICE_NAME, NO_PARAMS, "GET",
        Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME, mResource).run();
  }

  @Test
  public void serviceVersionTest() throws Exception {
    RestApiTestCaseFactory
        .newMasterTestCase(BlockMasterClientRestServiceHandler.SERVICE_VERSION, NO_PARAMS, "GET",
        Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION, mResource).run();
  }

  @Test
  public void getBlockInfoTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("blockId", "1");

    BlockInfo blockInfo = BlockInfoTest.createRandom();
    Mockito.doReturn(blockInfo).when(sBlockMaster).getBlockInfo(Mockito.anyLong());

    RestApiTestCaseFactory
        .newMasterTestCase(BlockMasterClientRestServiceHandler.GET_BLOCK_INFO, params, "GET",
        blockInfo, mResource).run();

    Mockito.verify(sBlockMaster).getBlockInfo(Mockito.anyLong());
  }

  @Test
  public void getCapacityBytesTest() throws Exception {
    Random random = new Random();
    long capacityBytes = random.nextLong();
    Mockito.doReturn(capacityBytes).when(sBlockMaster).getCapacityBytes();

    RestApiTestCaseFactory.newMasterTestCase(BlockMasterClientRestServiceHandler.GET_CAPACITY_BYTES,
        NO_PARAMS, "GET", capacityBytes, mResource).run();

    Mockito.verify(sBlockMaster).getCapacityBytes();
  }

  @Test
  public void getUsedBytesTest() throws Exception {
    Random random = new Random();
    long usedBytes = random.nextLong();
    Mockito.doReturn(usedBytes).when(sBlockMaster).getUsedBytes();

    RestApiTestCaseFactory
        .newMasterTestCase(BlockMasterClientRestServiceHandler.GET_USED_BYTES, NO_PARAMS, "GET",
        usedBytes, mResource).run();

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

    RestApiTestCaseFactory
        .newMasterTestCase(BlockMasterClientRestServiceHandler.GET_WORKER_INFO_LIST, NO_PARAMS,
        "GET", workerInfos, mResource).run();

    Mockito.verify(sBlockMaster).getWorkerInfoList();
  }
}
