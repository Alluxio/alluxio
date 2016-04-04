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

package alluxio.master.block;

import alluxio.LocalAlluxioClusterResource;
import alluxio.Constants;
import alluxio.master.AlluxioMaster;
import alluxio.rest.TestCaseFactory;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockInfoTest;

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

import java.util.Map;

/**
 * Test cases for {@link BlockMasterClientRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockMaster.class)
@Ignore
public class BlockMasterClientRestApiTest {
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
    return BlockMasterClientRestServiceHandler.SERVICE_PREFIX + "/" + suffix;
  }

  @Test
  public void serviceNameTest() throws Exception {
    TestCaseFactory
        .newMasterTestCase(getEndpoint(BlockMasterClientRestServiceHandler.SERVICE_NAME), NO_PARAMS,
            "GET", Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME, mResource).run();
  }

  @Test
  public void serviceVersionTest() throws Exception {
    TestCaseFactory
        .newMasterTestCase(getEndpoint(BlockMasterClientRestServiceHandler.SERVICE_VERSION),
            NO_PARAMS, "GET", Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION, mResource).run();
  }

  @Test
  public void getBlockInfoTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("blockId", "1");

    BlockInfo blockInfo = BlockInfoTest.createRandom();
    Mockito.doReturn(blockInfo).when(sBlockMaster).getBlockInfo(Mockito.anyLong());

    TestCaseFactory
        .newMasterTestCase(getEndpoint(BlockMasterClientRestServiceHandler.GET_BLOCK_INFO), params,
            "GET", blockInfo, mResource).run();

    Mockito.verify(sBlockMaster).getBlockInfo(Mockito.anyLong());
  }
}
