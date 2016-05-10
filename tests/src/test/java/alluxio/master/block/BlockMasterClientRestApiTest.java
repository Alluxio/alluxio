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

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.master.AlluxioMaster;
import alluxio.rest.TestCase;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockInfoTest;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link BlockMasterClientRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockMaster.class)
@Ignore("ALLUXIO-1888")
public class BlockMasterClientRestApiTest {
  private static final Map<String, String> NO_PARAMS = new HashMap<>();
  private BlockMaster mBlockMaster;
  private String mHostname;
  private int mPort;

  @Rule
  private LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource();

  @Before
  public void before() throws Exception {
    AlluxioMaster alluxioMaster = mResource.get().getMaster().getInternalMaster();
    mBlockMaster = PowerMockito.mock(BlockMaster.class);
    // Replace the block master created by LocalAlluxioClusterResource with a mock.
    BlockMaster blockMaster = Whitebox.getInternalState(alluxioMaster, "mBlockMaster");
    blockMaster.stop();
    Whitebox.setInternalState(alluxioMaster, "mBlockMaster", mBlockMaster);
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getMaster().getWebLocalPort();
  }

  private String getEndpoint(String suffix) {
    return BlockMasterClientRestServiceHandler.SERVICE_PREFIX + "/" + suffix;
  }

  @Test
  public void serviceNameTest() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(BlockMasterClientRestServiceHandler.SERVICE_NAME),
        NO_PARAMS, HttpMethod.GET, Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME).run();
  }

  @Test
  public void serviceVersionTest() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(BlockMasterClientRestServiceHandler.SERVICE_VERSION),
        NO_PARAMS, HttpMethod.GET, Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION).run();
  }

  @Test
  public void getBlockInfoTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("blockId", "1");

    BlockInfo blockInfo = BlockInfoTest.createRandom();
    Mockito.doReturn(blockInfo).when(mBlockMaster).getBlockInfo(Mockito.anyLong());

    new TestCase(mHostname, mPort, getEndpoint(BlockMasterClientRestServiceHandler.GET_BLOCK_INFO),
        params, HttpMethod.GET, blockInfo).run();

    Mockito.verify(mBlockMaster).getBlockInfo(Mockito.anyLong());
  }
}
