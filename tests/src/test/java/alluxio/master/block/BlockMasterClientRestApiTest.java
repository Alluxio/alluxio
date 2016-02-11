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

import alluxio.AbstractRestApiTest;
import alluxio.Constants;
import alluxio.master.AlluxioMaster;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockInfoTest;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerInfoTest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockMaster.class)
public class BlockMasterClientRestApiTest extends AbstractRestApiTest {

  @Override
  @Test
  public void endpointsTest() throws Exception {
    // Create test input values.
    Map<String, String> getBlockInfoParams = Maps.newHashMap();
    getBlockInfoParams.put("blockId", "1");

    // Generate random return values.
    Random random = new Random();
    BlockInfo blockInfo = BlockInfoTest.createRandom();
    long capacityBytes = random.nextLong();
    long usedBytes = random.nextLong();
    List<WorkerInfo> workerInfos = Lists.newArrayList();
    int numWorkerInfos = random.nextInt(10);
    for (int i = 0; i < numWorkerInfos; i++) {
      workerInfos.add(WorkerInfoTest.createRandom());
    }

    // Set up mocks.
    BlockMaster blockMaster = PowerMockito.mock(BlockMaster.class);
    Mockito.doReturn(blockInfo).when(blockMaster).getBlockInfo(Mockito.anyLong());
    Mockito.doReturn(capacityBytes).when(blockMaster).getCapacityBytes();
    Mockito.doReturn(usedBytes).when(blockMaster).getUsedBytes();
    Mockito.doReturn(workerInfos).when(blockMaster).getWorkerInfoList();
    AlluxioMaster alluxioMaster = PowerMockito.mock(AlluxioMaster.class);
    Mockito.doReturn(blockMaster).when(alluxioMaster).getBlockMaster();
    Whitebox.setInternalState(AlluxioMaster.class, "sAlluxioMaster", alluxioMaster);

    // Create test cases.
    List<TestCase> testCases = Lists.newArrayList();
    testCases.add(new MasterTestCase(BlockMasterClientRestServiceHandler.SERVICE_NAME,
        Maps.<String, String>newHashMap(), "GET", Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME));
    testCases.add(new MasterTestCase(BlockMasterClientRestServiceHandler.SERVICE_VERSION,
        Maps.<String, String>newHashMap(), "GET", Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION));
    testCases.add(
        new MasterTestCase(BlockMasterClientRestServiceHandler.GET_BLOCK_INFO, getBlockInfoParams,
            "GET", blockInfo));
    testCases.add(new MasterTestCase(BlockMasterClientRestServiceHandler.GET_CAPACITY_BYTES,
        Maps.<String, String>newHashMap(), "GET", capacityBytes));
    testCases.add(new MasterTestCase(BlockMasterClientRestServiceHandler.GET_USED_BYTES,
        Maps.<String, String>newHashMap(), "GET", usedBytes));
    testCases.add(new MasterTestCase(BlockMasterClientRestServiceHandler.GET_WORKER_INFO_LIST,
        Maps.<String, String>newHashMap(), "GET", workerInfos));

    // Execute test cases.
    run(testCases);

    // Verify invocations.
    Mockito.verify(blockMaster).getBlockInfo(Mockito.anyLong());
    Mockito.verify(blockMaster).getCapacityBytes();
    Mockito.verify(blockMaster).getUsedBytes();
    Mockito.verify(blockMaster).getWorkerInfoList();
  }
}
