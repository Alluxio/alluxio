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

package alluxio.worker.block;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.wire.LockBlockResult;
import alluxio.worker.block.io.BlockWriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Test cases for {@link BlockWorkerClientRestServiceHandler}.
 */
public final class BlockWorkerClientRestApiTest extends RestApiTest {
  private static final long SESSION_ID = 1;
  private static final long BLOCK_ID = 2;
  private static final String TIER_ALIAS = "MEM";
  private static final int INITIAL_BYTES = 5;
  private static final ByteBuffer BYTE_BUFFER = ByteBuffer.wrap("hello".getBytes());

  private BlockWorker mBlockWorker;

  @Before
  public void before() throws Exception {
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getWorkerProcess().getWebLocalPort();
    mServicePrefix = BlockWorkerClientRestServiceHandler.SERVICE_PREFIX;
    mBlockWorker = mResource.get().getWorkerProcess().getWorker(BlockWorker.class);
  }

  @Test
  public void serviceName() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.SERVICE_NAME),
        NO_PARAMS, HttpMethod.GET, Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME).run();
  }

  @Test
  public void serviceVersion() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.SERVICE_VERSION),
        NO_PARAMS, HttpMethod.GET, Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION).run();
  }

  @Test
  public void accessBlock() throws Exception {
    mBlockWorker.createBlock(SESSION_ID, BLOCK_ID, TIER_ALIAS, INITIAL_BYTES);
    mBlockWorker.commitBlock(SESSION_ID, BLOCK_ID);

    Map<String, String> params = new HashMap<>();
    params.put("blockId", Long.toString(BLOCK_ID));

    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.ACCESS_BLOCK),
        params, HttpMethod.POST, null).run();
  }

  @Test
  public void cacheBlock() throws Exception {
    mBlockWorker.createBlock(SESSION_ID, BLOCK_ID, TIER_ALIAS, INITIAL_BYTES);
    mBlockWorker.commitBlock(SESSION_ID, BLOCK_ID);

    Map<String, String> params = new HashMap<>();
    params.put("blockId", Long.toString(BLOCK_ID));
    params.put("sessionId", Long.toString(SESSION_ID));

    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.CACHE_BLOCK),
        params, HttpMethod.POST, null).run();
  }

  @Test
  public void cancelBlock() throws Exception {
    mBlockWorker.createBlock(SESSION_ID, BLOCK_ID, TIER_ALIAS, INITIAL_BYTES);

    Map<String, String> params = new HashMap<>();
    params.put("blockId", Long.toString(BLOCK_ID));
    params.put("sessionId", Long.toString(SESSION_ID));
    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.CANCEL_BLOCK),
        params, HttpMethod.POST, null).run();
  }

  @Test
  public void lockBlock() throws Exception {
    mBlockWorker.createBlock(SESSION_ID, BLOCK_ID, TIER_ALIAS, INITIAL_BYTES);
    mBlockWorker.commitBlock(SESSION_ID, BLOCK_ID);

    Map<String, String> params = new HashMap<>();
    params.put("blockId", Long.toString(BLOCK_ID));
    params.put("sessionId", Long.toString(SESSION_ID));
    String result =
        new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.LOCK_BLOCK),
            params, HttpMethod.POST, null).call();
    LockBlockResult lockBlockResult = new ObjectMapper().readValue(result, LockBlockResult.class);
    Assert.assertTrue(
        lockBlockResult.getBlockPath().contains(Configuration.get(PropertyKey.WORKER_DATA_FOLDER)));
  }

  @Test
  public void promoteBlock() throws Exception {
    mBlockWorker.createBlock(SESSION_ID, BLOCK_ID, TIER_ALIAS, INITIAL_BYTES);
    mBlockWorker.commitBlock(SESSION_ID, BLOCK_ID);

    Map<String, String> params = new HashMap<>();
    params.put("blockId", Long.toString(BLOCK_ID));
    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.PROMOTE_BLOCK),
        params, HttpMethod.POST, null).run();
  }

  @Test
  public void readBlock() throws Exception {
    // Write a block and acquire a lock for it.
    mBlockWorker.createBlock(SESSION_ID, BLOCK_ID, TIER_ALIAS, INITIAL_BYTES);
    BlockWriter writer = mBlockWorker.getTempBlockWriterRemote(SESSION_ID, BLOCK_ID);
    writer.append(BYTE_BUFFER);
    writer.close();
    mBlockWorker.commitBlock(SESSION_ID, BLOCK_ID);
    long lockId = mBlockWorker.lockBlock(SESSION_ID, BLOCK_ID);

    Map<String, String> params = new HashMap<>();
    params.put("blockId", Long.toString(BLOCK_ID));
    params.put("sessionId", Long.toString(SESSION_ID));
    params.put("lockId", Long.toString(lockId));
    params.put("offset", "0");
    params.put("length", Long.toString(INITIAL_BYTES));

    TestCase testCase =
        new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.READ_BLOCK),
            params, HttpMethod.GET, BYTE_BUFFER);

    HttpURLConnection connection = (HttpURLConnection) testCase.createURL().openConnection();
    connection.setRequestMethod(testCase.getMethod());
    connection.connect();
    Assert.assertEquals(testCase.getEndpoint(), Response.Status.OK.getStatusCode(),
        connection.getResponseCode());
    Assert.assertEquals(new String(BYTE_BUFFER.array()), testCase.getResponse(connection));
  }

  @Test
  public void requestBlockLocation() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("blockId", "1");
    params.put("sessionId", "1");
    params.put("initialBytes", "1");
    String location = new TestCase(mHostname, mPort,
        getEndpoint(BlockWorkerClientRestServiceHandler.REQUEST_BLOCK_LOCATION), params,
        HttpMethod.POST, null).call();
    Assert.assertTrue(location.contains(Configuration.get(PropertyKey.WORKER_DATA_FOLDER)));
  }

  @Test
  public void requestSpace() throws Exception {
    mBlockWorker.createBlock(SESSION_ID, BLOCK_ID, TIER_ALIAS, INITIAL_BYTES);

    Map<String, String> params = new HashMap<>();
    params.put("blockId", Long.toString(BLOCK_ID));
    params.put("sessionId", Long.toString(SESSION_ID));
    params.put("requestBytes", "10");
    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.REQUEST_SPACE),
        params, HttpMethod.POST, null).run();
  }

  @Test
  public void unlockBlock() throws Exception {
    // Write a block and acquire a lock for it.
    mBlockWorker.createBlock(SESSION_ID, BLOCK_ID, TIER_ALIAS, INITIAL_BYTES);
    BlockWriter writer = mBlockWorker.getTempBlockWriterRemote(SESSION_ID, BLOCK_ID);
    writer.append(BYTE_BUFFER);
    writer.close();
    mBlockWorker.commitBlock(SESSION_ID, BLOCK_ID);
    mBlockWorker.lockBlock(SESSION_ID, BLOCK_ID);

    Map<String, String> params = new HashMap<>();
    params.put("blockId", Long.toString(BLOCK_ID));
    params.put("sessionId", Long.toString(SESSION_ID));

    new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.UNLOCK_BLOCK),
        params, HttpMethod.POST, null).run();

  }

  @Test
  public void writeBlock() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("blockId", Long.toString(BLOCK_ID));
    params.put("sessionId", Long.toString(SESSION_ID));
    params.put("offset", "0");
    params.put("length", Long.toString(INITIAL_BYTES));

    TestCase testCase =
        new TestCase(mHostname, mPort, getEndpoint(BlockWorkerClientRestServiceHandler.WRITE_BLOCK),
            params, HttpMethod.POST, null);

    HttpURLConnection connection = (HttpURLConnection) testCase.createURL().openConnection();
    connection.setRequestProperty("Content-Type", MediaType.APPLICATION_OCTET_STREAM);
    connection.setRequestMethod(testCase.getMethod());
    connection.setDoOutput(true);
    connection.connect();
    connection.getOutputStream().write(BYTE_BUFFER.array());
    Assert.assertEquals(testCase.getEndpoint(), Response.Status.OK.getStatusCode(),
        connection.getResponseCode());
    Assert.assertEquals("", testCase.getResponse(connection));

    // Verify that the right data was written.
    mBlockWorker.commitBlock(SESSION_ID, BLOCK_ID);
    long lockId = mBlockWorker.lockBlock(SESSION_ID, BLOCK_ID);
    String file = mBlockWorker.readBlock(SESSION_ID, BLOCK_ID, lockId);
    byte[] result = new byte[INITIAL_BYTES];
    IOUtils.readFully(new FileInputStream(file), result);
    Assert.assertArrayEquals(BYTE_BUFFER.array(), result);
  }
}
