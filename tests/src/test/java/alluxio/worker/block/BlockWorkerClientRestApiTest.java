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

package alluxio.worker.block;

import alluxio.Constants;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.util.CommonUtils;
import alluxio.wire.LockBlockResult;
import alluxio.wire.LockBlockResultTest;
import alluxio.worker.AlluxioWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Test cases for {@link BlockWorkerClientRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioWorker.class, BlockReader.class, BlockWorker.class, BlockWriter.class})
public final class BlockWorkerClientRestApiTest extends RestApiTest {
  private BlockWorker mBlockWorker;

  @Before
  public void before() throws Exception {
    AlluxioWorker alluxioWorker = mResource.get().getWorker();
    mBlockWorker = PowerMockito.mock(BlockWorker.class);
    // Replace the block worker created by LocalAlluxioClusterResource with a mock.
    BlockWorker blockWorker = Whitebox.getInternalState(alluxioWorker, "mBlockWorker");
    blockWorker.stop();
    Whitebox.setInternalState(alluxioWorker, "mBlockWorker", mBlockWorker);
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getWorker().getWebLocalPort();
    mServicePrefix = BlockWorkerClientRestServiceHandler.SERVICE_PREFIX;
  }

  @Test
  public void serviceNameTest() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.SERVICE_NAME),
        NO_PARAMS, HttpMethod.GET, Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME).run();
  }

  @Test
  public void serviceVersionTest() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.SERVICE_VERSION),
        NO_PARAMS, HttpMethod.GET, Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION).run();
  }

  @Test
  public void accessBlockTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("blockId", "1");

    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.ACCESS_BLOCK),
        params, HttpMethod.POST, null).run();

    Mockito.verify(mBlockWorker).accessBlock(Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void asyncCheckpointTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("fileId", "1");

    new TestCase(mHostname, mPort,
        getEndpoint(BlockWorkerClientRestServiceHandler.ASYNC_CHECKPOINT), params, HttpMethod.POST,
        false).run();
  }

  @Test
  public void cacheBlockTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("blockId", "1");
    params.put("sessionId", "1");

    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.CACHE_BLOCK),
        params, HttpMethod.POST, null).run();

    Mockito.verify(mBlockWorker).commitBlock(Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void cancelBlockTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("blockId", "1");
    params.put("sessionId", "1");

    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.CANCEL_BLOCK),
        params, HttpMethod.POST, null).run();

    Mockito.verify(mBlockWorker).abortBlock(Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void lockBlockTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("blockId", "1");
    params.put("sessionId", "1");

    LockBlockResult lockBlockResult = LockBlockResultTest.createRandom();
    Mockito.doReturn(lockBlockResult.getLockId()).when(mBlockWorker)
        .lockBlock(Mockito.anyLong(), Mockito.anyLong());
    Mockito.doReturn(lockBlockResult.getBlockPath()).when(mBlockWorker)
        .readBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());

    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.LOCK_BLOCK),
        params, HttpMethod.POST, lockBlockResult).run();

    Mockito.verify(mBlockWorker).lockBlock(Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(mBlockWorker).readBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void promoteBlockTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("blockId", "1");

    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.PROMOTE_BLOCK),
        params, HttpMethod.POST, null).run();

    Mockito.verify(mBlockWorker)
        .moveBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString());
  }

  @Test
  public void readBlockTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("blockId", "1");
    params.put("sessionId", "1");
    params.put("lockId", "1");
    params.put("offset", "0");
    params.put("length", "-1");

    Random random = new Random();
    byte[] bytes = CommonUtils.randomBytes(random.nextInt(64));
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

    BlockReader blockReader = PowerMockito.mock(BlockReader.class);
    Mockito.doReturn(byteBuffer).when(blockReader).read(Mockito.anyLong(), Mockito.anyLong());
    Mockito.doReturn((long) bytes.length).when(blockReader).getLength();
    Mockito.doReturn(blockReader).when(mBlockWorker)
        .readBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());

    TestCase testCase =
        new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.READ_BLOCK),
            params, HttpMethod.GET, byteBuffer);

    HttpURLConnection connection = (HttpURLConnection) testCase.createURL().openConnection();
    connection.setRequestMethod(testCase.getMethod());
    connection.connect();
    Assert.assertEquals(testCase.getEndpoint(), connection.getResponseCode(),
        Response.Status.OK.getStatusCode());
    Assert.assertEquals(new String(byteBuffer.array()), testCase.getResponse(connection));

    Mockito.verify(mBlockWorker)
        .readBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void requestBlockLocationTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("blockId", "1");
    params.put("sessionId", "1");
    params.put("initialBytes", "1");

    String blockLocation = CommonUtils.randomString(10);
    Mockito.doReturn(blockLocation).when(mBlockWorker)
        .createBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyLong());

    new TestCase(mHostname, mPort,
        getEndpoint(BlockWorkerClientRestServiceHandler.REQUEST_BLOCK_LOCATION), params,
        HttpMethod.POST, blockLocation).run();

    Mockito.verify(mBlockWorker)
        .createBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyLong());

  }

  @Test
  public void requestSpaceTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("blockId", "1");
    params.put("sessionId", "1");
    params.put("requestBytes", "1");

    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.REQUEST_SPACE),
        params, HttpMethod.POST, null).run();

    Mockito.verify(mBlockWorker)
        .requestSpace(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void unlockBlockTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("blockId", "1");
    params.put("sessionId", "1");

    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.UNLOCK_BLOCK),
        params, HttpMethod.POST, null).run();

    Mockito.verify(mBlockWorker).unlockBlock(Mockito.anyLong(), Mockito.anyLong());

  }

  @Test
  public void writeBlockTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("blockId", "1");
    params.put("sessionId", "1");
    params.put("offset", "0");
    params.put("length", "-1");

    Random random = new Random();
    byte[] bytes = CommonUtils.randomBytes(random.nextInt(64));

    BlockWriter blockWriter = PowerMockito.mock(BlockWriter.class);
    Mockito.doReturn(blockWriter).when(mBlockWorker)
        .getTempBlockWriterRemote(Mockito.anyLong(), Mockito.anyLong());

    TestCase testCase =
        new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.WRITE_BLOCK),
            params, HttpMethod.POST, null);

    HttpURLConnection connection = (HttpURLConnection) testCase.createURL().openConnection();
    connection.setRequestProperty("Content-Type", MediaType.APPLICATION_OCTET_STREAM);
    connection.setRequestMethod(testCase.getMethod());
    connection.setDoOutput(true);
    connection.connect();
    connection.getOutputStream().write(bytes);
    Assert.assertEquals(testCase.getEndpoint(), Response.Status.OK.getStatusCode(),
        connection.getResponseCode());
    Assert.assertEquals("", testCase.getResponse(connection));

    Mockito.verify(mBlockWorker).getTempBlockWriterRemote(Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(blockWriter).append(ByteBuffer.wrap(bytes));
  }
}
