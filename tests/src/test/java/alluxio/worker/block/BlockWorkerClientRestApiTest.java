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

import alluxio.AbstractRestApiTest;
import alluxio.Constants;
import alluxio.wire.LockBlockResult;
import alluxio.wire.LockBlockResultTest;
import alluxio.worker.AlluxioWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.ws.rs.core.Response;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioWorker.class, BlockReader.class, BlockWorker.class, BlockWriter.class})
public class BlockWorkerClientRestApiTest extends AbstractRestApiTest {

  @Override
  @Test
  public void endpointsTest() throws Exception {
    // Create test input values.
    Map<String, String> accessBlockParams = Maps.newHashMap();
    accessBlockParams.put("blockId", "1");
    Map<String, String> asyncCheckpointParams = Maps.newHashMap();
    asyncCheckpointParams.put("fileId", "1");
    Map<String, String> cacheBlockParams = Maps.newHashMap();
    cacheBlockParams.put("blockId", "1");
    cacheBlockParams.put("sessionId", "1");
    Map<String, String> cancelBlockParams = Maps.newHashMap();
    cancelBlockParams.put("blockId", "1");
    cancelBlockParams.put("sessionId", "1");
    Map<String, String> lockBlockParams = Maps.newHashMap();
    lockBlockParams.put("blockId", "1");
    lockBlockParams.put("sessionId", "1");
    Map<String, String> promoteBlockParams = Maps.newHashMap();
    promoteBlockParams.put("blockId", "1");
    Map<String, String> requestBlockLocationParams = Maps.newHashMap();
    requestBlockLocationParams.put("blockId", "1");
    requestBlockLocationParams.put("sessionId", "1");
    requestBlockLocationParams.put("initialBytes", "1");
    Map<String, String> requestSpaceParams = Maps.newHashMap();
    requestSpaceParams.put("blockId", "1");
    requestSpaceParams.put("sessionId", "1");
    requestSpaceParams.put("requestBytes", "1");
    Map<String, String> unlockBlockParams = Maps.newHashMap();
    unlockBlockParams.put("blockId", "1");
    unlockBlockParams.put("sessionId", "1");

    // Generate random return values.
    Random random = new Random();
    LockBlockResult lockBlockResult = LockBlockResultTest.createRandom();
    String blockLocation = "";
    int blockLocationLength = random.nextInt(10);
    for (int i = 0; i < blockLocationLength; i++) {
      blockLocation += random.nextInt(96) + 32; // generates a random alphanumeric symbol
    }

    // Set up mocks.
    BlockWorker blockWorker = PowerMockito.mock(BlockWorker.class);
    Mockito.doReturn(blockLocation).when(blockWorker)
        .createBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyLong());
    Mockito.doReturn(lockBlockResult.getLockId()).when(blockWorker)
        .lockBlock(Mockito.anyLong(), Mockito.anyLong());
    Mockito.doReturn(lockBlockResult.getBlockPath()).when(blockWorker)
        .readBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
    AlluxioWorker alluxioWorker = PowerMockito.mock(AlluxioWorker.class);
    Mockito.doReturn(blockWorker).when(alluxioWorker).getBlockWorker();
    Whitebox.setInternalState(AlluxioWorker.class, "sAlluxioWorker", alluxioWorker);

    // Create test cases.
    List<TestCase> testCases = Lists.newArrayList();
    testCases.add(new WorkerTestCase(BlockWorkerClientRestServiceHandler.SERVICE_NAME,
        Maps.<String, String>newHashMap(), "GET", Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME));
    testCases.add(new WorkerTestCase(BlockWorkerClientRestServiceHandler.SERVICE_VERSION,
        Maps.<String, String>newHashMap(), "GET", Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION));
    testCases.add(
        new WorkerTestCase(BlockWorkerClientRestServiceHandler.ACCESS_BLOCK, accessBlockParams,
            "POST", ""));
    testCases.add(new WorkerTestCase(BlockWorkerClientRestServiceHandler.ASYNC_CHECKPOINT,
        asyncCheckpointParams, "POST", "false"));
    testCases.add(
        new WorkerTestCase(BlockWorkerClientRestServiceHandler.CACHE_BLOCK, cacheBlockParams,
            "POST", ""));
    testCases.add(
        new WorkerTestCase(BlockWorkerClientRestServiceHandler.CANCEL_BLOCK, cancelBlockParams,
            "POST", ""));
    testCases.add(
        new WorkerTestCase(BlockWorkerClientRestServiceHandler.LOCK_BLOCK, lockBlockParams, "POST",
            lockBlockResult));
    testCases.add(
        new WorkerTestCase(BlockWorkerClientRestServiceHandler.PROMOTE_BLOCK, promoteBlockParams,
            "POST", ""));
    testCases.add(new WorkerTestCase(BlockWorkerClientRestServiceHandler.REQUEST_BLOCK_LOCATION,
        requestBlockLocationParams, "POST", blockLocation));
    testCases.add(
        new WorkerTestCase(BlockWorkerClientRestServiceHandler.REQUEST_SPACE, requestSpaceParams,
            "POST", ""));
    testCases.add(
        new WorkerTestCase(BlockWorkerClientRestServiceHandler.UNLOCK_BLOCK, unlockBlockParams,
            "POST", ""));

    // Execute test cases.
    run(testCases);

    // Verify invocations.
    Mockito.verify(blockWorker).abortBlock(Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(blockWorker).accessBlock(Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(blockWorker).commitBlock(Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(blockWorker)
        .createBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyLong());
    Mockito.verify(blockWorker).lockBlock(Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(blockWorker)
        .moveBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString());
    Mockito.verify(blockWorker).readBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(blockWorker)
        .requestSpace(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(blockWorker).unlockBlock(Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void readTest() throws Exception {
    // Create test input values.
    Map<String, String> readBlockParams = Maps.newHashMap();
    readBlockParams.put("blockId", "1");
    readBlockParams.put("sessionId", "1");
    readBlockParams.put("lockId", "1");
    readBlockParams.put("offset", "1");
    readBlockParams.put("length", "-1");

    // Generate random return values.
    Random random = new Random();
    byte[] bytes = new byte[64];
    random.nextBytes(bytes);
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

    // Set up mocks.
    BlockReader blockReader = PowerMockito.mock(BlockReader.class);
    Mockito.doReturn(byteBuffer).when(blockReader).read(Mockito.anyLong(), Mockito.anyLong());
    Mockito.doReturn((long) 1).when(blockReader).getLength();
    BlockWorker blockWorker = PowerMockito.mock(BlockWorker.class);
    Mockito.doReturn(blockReader).when(blockWorker)
        .readBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
    AlluxioWorker alluxioWorker = PowerMockito.mock(AlluxioWorker.class);
    Mockito.doReturn(blockWorker).when(alluxioWorker).getBlockWorker();
    Whitebox.setInternalState(AlluxioWorker.class, "sAlluxioWorker", alluxioWorker);

    // Create the test case.
    WorkerTestCase testCase =
        new WorkerTestCase(BlockWorkerClientRestServiceHandler.READ_BLOCK, readBlockParams, "GET",
            "");

    // Execute the test case.
    HttpURLConnection connection = (HttpURLConnection) createURL(testCase).openConnection();
    connection.setRequestMethod(testCase.getMethod());
    connection.connect();
    Assert.assertEquals(testCase.getSuffix(), connection.getResponseCode(),
        Response.Status.OK.getStatusCode());
    Assert.assertEquals(new String(byteBuffer.array()), getResponse(connection));

    // Verify invocations.
    Mockito.verify(blockWorker)
        .readBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(blockWorker).accessBlock(Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void writeTest() throws Exception {
    // Create test input values.
    Map<String, String> writeBlockParams = Maps.newHashMap();
    writeBlockParams.put("blockId", "1");
    writeBlockParams.put("sessionId", "1");
    writeBlockParams.put("offset", "1");
    writeBlockParams.put("length", "-1");
    Random random = new Random();
    byte[] bytes = new byte[64];
    random.nextBytes(bytes);

    // Set up mocks.
    BlockWriter blockWriter = PowerMockito.mock(BlockWriter.class);
    BlockWorker blockWorker = PowerMockito.mock(BlockWorker.class);
    Mockito.doReturn(blockWriter).when(blockWorker)
        .getTempBlockWriterRemote(Mockito.anyLong(), Mockito.anyLong());
    AlluxioWorker alluxioWorker = PowerMockito.mock(AlluxioWorker.class);
    Mockito.doReturn(blockWorker).when(alluxioWorker).getBlockWorker();
    Whitebox.setInternalState(AlluxioWorker.class, "sAlluxioWorker", alluxioWorker);

    // Create the test case.
    WorkerTestCase testCase =
        new WorkerTestCase(BlockWorkerClientRestServiceHandler.WRITE_BLOCK, writeBlockParams, "PUT",
            "");

    // Execute the test case.
    HttpURLConnection connection = (HttpURLConnection) createURL(testCase).openConnection();
    connection.setRequestMethod(testCase.getMethod());
    connection.setDoOutput(true);
    connection.connect();
    connection.getOutputStream().write(bytes);
    Assert.assertEquals(testCase.getSuffix(), connection.getResponseCode(),
        Response.Status.OK.getStatusCode());
    Assert.assertEquals("", getResponse(connection));

    // Verify invocations.
    Mockito.verify(blockWorker).getTempBlockWriterRemote(Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(blockWriter).append(ByteBuffer.wrap(bytes));
  }
}
