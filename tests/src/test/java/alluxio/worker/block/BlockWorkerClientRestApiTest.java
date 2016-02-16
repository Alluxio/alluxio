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

package alluxio.worker.block;

import alluxio.LocalAlluxioClusterResource;
import alluxio.RestApiTestCase;
import alluxio.RestApiTestCaseFactory;
import alluxio.Constants;
import alluxio.wire.LockBlockResult;
import alluxio.wire.LockBlockResultTest;
import alluxio.worker.AlluxioWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Test cases for {@link BlockWorkerClientRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioWorker.class, BlockReader.class, BlockWorker.class, BlockWriter.class})
public class BlockWorkerClientRestApiTest {
  private static final Map<String, String> NO_PARAMS = Maps.<String, String>newHashMap();
  private static BlockWorker sBlockWorker;

  @Rule
  private LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource();

  @BeforeClass
  public static void beforeClass() {
    sBlockWorker = PowerMockito.mock(BlockWorker.class);
    AlluxioWorker alluxioWorker = PowerMockito.mock(AlluxioWorker.class);
    Mockito.doReturn(sBlockWorker).when(alluxioWorker).getBlockWorker();
    Whitebox.setInternalState(AlluxioWorker.class, "sAlluxioWorker", alluxioWorker);
  }

  @Test
  public void serviceNameTest() throws Exception {
    RestApiTestCaseFactory
        .newWorkerTestCase(BlockWorkerClientRestServiceHandler.SERVICE_NAME, NO_PARAMS, "GET",
        Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME, mResource).run();
  }

  @Test
  public void serviceVersionTest() throws Exception {
    RestApiTestCaseFactory
        .newWorkerTestCase(BlockWorkerClientRestServiceHandler.SERVICE_VERSION, NO_PARAMS, "GET",
        Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION, mResource).run();
  }

  @Test
  public void accessBlockTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("blockId", "1");

    RestApiTestCaseFactory
        .newWorkerTestCase(BlockWorkerClientRestServiceHandler.ACCESS_BLOCK, params, "POST",
        "", mResource).run();

    Mockito.verify(sBlockWorker).accessBlock(Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void asyncCheckpointTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("fileId", "1");

    RestApiTestCaseFactory
        .newWorkerTestCase(BlockWorkerClientRestServiceHandler.ASYNC_CHECKPOINT, params, "POST",
        "false", mResource).run();
  }

  @Test
  public void cacheBlockTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("blockId", "1");
    params.put("sessionId", "1");

    RestApiTestCaseFactory
        .newWorkerTestCase(BlockWorkerClientRestServiceHandler.CACHE_BLOCK, params, "POST",
        "", mResource).run();

    Mockito.verify(sBlockWorker).commitBlock(Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void cancelBlockTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("blockId", "1");
    params.put("sessionId", "1");

    RestApiTestCaseFactory
        .newWorkerTestCase(BlockWorkerClientRestServiceHandler.CANCEL_BLOCK, params, "POST",
        "", mResource).run();

    Mockito.verify(sBlockWorker).abortBlock(Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void lockBlockTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("blockId", "1");
    params.put("sessionId", "1");

    LockBlockResult lockBlockResult = LockBlockResultTest.createRandom();
    Mockito.doReturn(lockBlockResult.getLockId()).when(sBlockWorker)
        .lockBlock(Mockito.anyLong(), Mockito.anyLong());
    Mockito.doReturn(lockBlockResult.getBlockPath()).when(sBlockWorker)
        .readBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());

    RestApiTestCaseFactory
        .newWorkerTestCase(BlockWorkerClientRestServiceHandler.LOCK_BLOCK, params, "POST",
        lockBlockResult, mResource).run();

    Mockito.verify(sBlockWorker).lockBlock(Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(sBlockWorker).readBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void promoteBlockTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("blockId", "1");

    RestApiTestCaseFactory
        .newWorkerTestCase(BlockWorkerClientRestServiceHandler.PROMOTE_BLOCK, params,
        "POST", "", mResource).run();

    Mockito.verify(sBlockWorker)
        .moveBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString());
  }

  @Test
  public void readBlockTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("blockId", "1");
    params.put("sessionId", "1");
    params.put("lockId", "1");
    params.put("offset", "1");
    params.put("length", "-1");

    Random random = new Random();
    String buffer = "";
    int bufferLength = random.nextInt(64);
    for (int i = 0; i < bufferLength; i++) {
      buffer += random.nextInt(96) + 32; // generates a random alphanumeric symbol
    }
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer.getBytes());

    BlockReader blockReader = PowerMockito.mock(BlockReader.class);
    Mockito.doReturn(byteBuffer).when(blockReader).read(Mockito.anyLong(), Mockito.anyLong());
    Mockito.doReturn((long) 1).when(blockReader).getLength();
    Mockito.doReturn(blockReader).when(sBlockWorker)
        .readBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());

    RestApiTestCase testCase = RestApiTestCaseFactory
        .newWorkerTestCase(BlockWorkerClientRestServiceHandler.READ_BLOCK, params, "GET",
            byteBuffer, mResource);

    HttpURLConnection connection = (HttpURLConnection) testCase.createURL().openConnection();
    connection.setRequestMethod(testCase.getMethod());
    connection.connect();
    Assert.assertEquals(testCase.getSuffix(), connection.getResponseCode(),
        Response.Status.OK.getStatusCode());
    Assert.assertEquals(new String(byteBuffer.array()), testCase.getResponse(connection));

    // Verify invocations.
    Mockito.verify(sBlockWorker)
        .readBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void requestBlockLocationTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("blockId", "1");
    params.put("sessionId", "1");
    params.put("initialBytes", "1");

    Random random = new Random();
    String blockLocation = "";
    int blockLocationLength = random.nextInt(10);
    for (int i = 0; i < blockLocationLength; i++) {
      blockLocation += random.nextInt(96) + 32; // generates a random alphanumeric symbol
    }
    Mockito.doReturn(blockLocation).when(sBlockWorker)
        .createBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyLong());

    RestApiTestCaseFactory
        .newWorkerTestCase(BlockWorkerClientRestServiceHandler.REQUEST_BLOCK_LOCATION, params,
            "POST", blockLocation, mResource).run();

    Mockito.verify(sBlockWorker)
        .createBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyLong());

  }

  @Test
  public void requestSpaceTest() throws Exception {
    // Create test input values.
    Map<String, String> params = Maps.newHashMap();
    params.put("blockId", "1");
    params.put("sessionId", "1");
    params.put("requestBytes", "1");

    RestApiTestCaseFactory
        .newWorkerTestCase(BlockWorkerClientRestServiceHandler.REQUEST_SPACE, params, "POST", "",
        mResource).run();

    Mockito.verify(sBlockWorker)
        .requestSpace(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void unlockBlockTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("blockId", "1");
    params.put("sessionId", "1");

    RestApiTestCaseFactory
        .newWorkerTestCase(BlockWorkerClientRestServiceHandler.UNLOCK_BLOCK, params, "POST", "",
        mResource).run();

    Mockito.verify(sBlockWorker).unlockBlock(Mockito.anyLong(), Mockito.anyLong());

  }

  @Test
  public void writeTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("blockId", "1");
    params.put("sessionId", "1");
    params.put("offset", "1");
    params.put("length", "-1");

    Random random = new Random();
    String buffer = "";
    int bufferLength = random.nextInt(64);
    for (int i = 0; i < bufferLength; i++) {
      buffer += random.nextInt(96) + 32; // generates a random alphanumeric symbol
    }

    BlockWriter blockWriter = PowerMockito.mock(BlockWriter.class);
    Mockito.doReturn(blockWriter).when(sBlockWorker)
        .getTempBlockWriterRemote(Mockito.anyLong(), Mockito.anyLong());

    // Create the test case.
    RestApiTestCase testCase = RestApiTestCaseFactory
        .newWorkerTestCase(BlockWorkerClientRestServiceHandler.WRITE_BLOCK, params, "POST", "",
            mResource);

    HttpURLConnection connection = (HttpURLConnection) testCase.createURL().openConnection();
    connection.setRequestProperty("Content-Type", MediaType.APPLICATION_OCTET_STREAM);
    connection.setRequestMethod(testCase.getMethod());
    connection.setDoOutput(true);
    connection.connect();
    connection.getOutputStream().write(buffer.getBytes());
    Assert.assertEquals(testCase.getSuffix(), Response.Status.OK.getStatusCode(),
        connection.getResponseCode());
    Assert.assertEquals("", testCase.getResponse(connection));

    // Verify invocations.
    Mockito.verify(sBlockWorker).getTempBlockWriterRemote(Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(blockWriter).append(ByteBuffer.wrap(buffer.getBytes()));
  }
}
