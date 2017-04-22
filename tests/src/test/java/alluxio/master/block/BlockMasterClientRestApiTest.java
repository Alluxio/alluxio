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

package alluxio.master.block;

import alluxio.Constants;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.wire.BlockInfo;
import alluxio.worker.block.BlockWorker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link BlockMasterClientRestServiceHandler}.
 */
public final class BlockMasterClientRestApiTest extends RestApiTest {

  @Before
  public void before() throws Exception {
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getLocalAlluxioMaster().getMasterProcess().getWebAddress().getPort();
    mServicePrefix = BlockMasterClientRestServiceHandler.SERVICE_PREFIX;
  }

  @Test
  public void serviceName() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(BlockMasterClientRestServiceHandler.SERVICE_NAME),
        NO_PARAMS, HttpMethod.GET, Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME).run();
  }

  @Test
  public void serviceVersion() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(BlockMasterClientRestServiceHandler.SERVICE_VERSION),
        NO_PARAMS, HttpMethod.GET, Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION).run();
  }

  @Test
  public void getBlockInfo() throws Exception {
    long sessionId = 1;
    long blockId = 2;
    String tierAlias = "MEM";
    long initialBytes = 3;

    BlockWorker blockWorker = mResource.get().getWorkerProcess().getWorker(BlockWorker.class);
    String file = blockWorker.createBlock(sessionId, blockId, tierAlias, initialBytes);
    FileOutputStream outStream = new FileOutputStream(file);
    outStream.write("abc".getBytes());
    outStream.close();
    blockWorker.commitBlock(sessionId, blockId);

    Map<String, String> params = new HashMap<>();
    params.put("blockId", Long.toString(blockId));
    String response = new TestCase(mHostname, mPort,
        getEndpoint(BlockMasterClientRestServiceHandler.GET_BLOCK_INFO), params, HttpMethod.GET,
        null).call();
    BlockInfo blockInfo = new ObjectMapper().readValue(response, BlockInfo.class);
    Assert.assertEquals(blockId, blockInfo.getBlockId());
    Assert.assertEquals(initialBytes, blockInfo.getLength());
    Assert.assertEquals("MEM", Iterables.getOnlyElement(blockInfo.getLocations()).getTierAlias());
  }
}
