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

package alluxio.worker.netty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.client.netty.ClientHandler;
import alluxio.client.netty.NettyClient;
import alluxio.client.netty.SingleResponseListener;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.RPCBlockWriteRequest;
import alluxio.network.protocol.RPCFileReadRequest;
import alluxio.network.protocol.RPCFileWriteRequest;
import alluxio.network.protocol.RPCRequest;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataByteArrayChannel;
import alluxio.worker.AlluxioWorkerService;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.MockBlockReader;
import alluxio.worker.block.io.MockBlockWriter;
import alluxio.worker.file.FileSystemWorker;

import com.google.common.base.Charsets;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link NettyDataServer}. The entire alluxio.worker.netty package is treated as one
 * unit for the purposes of these tests; all classes except for NettyDataServer are package-private.
 */
public final class NettyDataServerTest {
  private NettyDataServer mNettyDataServer;
  private BlockWorker mBlockWorker;
  private FileSystemWorker mFileSystemWorker;

  @Before
  public void before() {
    mBlockWorker = Mockito.mock(BlockWorker.class);
    mFileSystemWorker = Mockito.mock(FileSystemWorker.class);
    AlluxioWorkerService alluxioWorker = Mockito.mock(AlluxioWorkerService.class);
    Mockito.when(alluxioWorker.getBlockWorker()).thenReturn(mBlockWorker);
    Mockito.when(alluxioWorker.getFileSystemWorker()).thenReturn(mFileSystemWorker);

    mNettyDataServer = new NettyDataServer(new InetSocketAddress(0), alluxioWorker);
  }

  @After
  public void after() throws Exception {
    mNettyDataServer.close();
  }

  @Test
  public void closeTest() throws Exception {
    mNettyDataServer.close();
  }

  @Test
  public void portTest() {
    assertTrue(mNettyDataServer.getPort() > 0);
  }

  @Test
  public void readBlock() throws Exception {
    long sessionId = 0;
    long blockId = 1;
    long offset = 2;
    long length = 3;
    long lockId = 4;
    when(mBlockWorker.readBlockRemote(sessionId, blockId, lockId)).thenReturn(
        new MockBlockReader("abcdefg".getBytes(Charsets.UTF_8)));
    RPCResponse response =
        request(new RPCBlockReadRequest(blockId, offset, length, lockId, sessionId));

    // Verify that the 3 bytes were read at offset 2.
    assertEquals("cde",
        Charsets.UTF_8.decode(response.getPayloadDataBuffer().getReadOnlyByteBuffer()).toString());
  }

  @Test
  public void blockWorkerExceptionCausesReadFailedStatus() throws Exception {
    when(mBlockWorker.readBlockRemote(anyLong(), anyLong(), anyLong()))
        .thenThrow(new RuntimeException());
    RPCResponse response = request(new RPCBlockReadRequest(1, 2, 3, 4, 0));

    // Verify that the read request failed with UFS_READ_FAILED status.
    assertEquals(RPCResponse.Status.UFS_READ_FAILED, response.getStatus());
  }

  @Test
  public void blockWorkerBlockDoesNotExistExceptionCausesFileDneStatus() throws Exception {
    when(mBlockWorker.readBlockRemote(anyLong(), anyLong(), anyLong()))
        .thenThrow(new BlockDoesNotExistException(""));
    RPCResponse response = request(new RPCBlockReadRequest(1, 2, 3, 4, 0));

    // Verify that the read request failed with a FILE_DNE status.
    assertEquals(RPCResponse.Status.FILE_DNE, response.getStatus());
  }

  @Test
  public void blockWorkerExceptionCausesFailStatusOnRead() throws Exception {
    when(mBlockWorker.readBlockRemote(anyLong(), anyLong(), anyLong()))
        .thenThrow(new RuntimeException());
    RPCResponse response = request(new RPCBlockReadRequest(1, 2, 3, 4, 0));

    // Verify that the write request failed.
    assertEquals(RPCResponse.Status.UFS_READ_FAILED, response.getStatus());
  }

  @Test
  public void writeNewBlock() throws Exception {
    long sessionId = 0;
    long blockId = 1;
    long length = 2;
    // Offset is set to 0 so that a new block will be created.
    long offset = 0;
    DataByteArrayChannel data = new DataByteArrayChannel("abc".getBytes(Charsets.UTF_8), 0, 3);
    MockBlockWriter blockWriter = new MockBlockWriter();
    when(mBlockWorker.getTempBlockWriterRemote(sessionId, blockId)).thenReturn(blockWriter);
    RPCResponse response =
        request(new RPCBlockWriteRequest(sessionId, blockId, offset, length, data));

    // Verify that the write request tells the worker to create a new block and write the specified
    // data to it.
    assertEquals(RPCResponse.Status.SUCCESS, response.getStatus());
    verify(mBlockWorker).createBlockRemote(sessionId, blockId, "MEM", length);
    assertEquals("ab", new String(blockWriter.getBytes(), Charsets.UTF_8));
  }

  @Test
  public void writeExistingBlock() throws Exception {
    long sessionId = 0;
    long blockId = 1;
    // Offset is set to 1 so that the write is directed to an existing block
    long offset = 1;
    long length = 2;
    DataByteArrayChannel data = new DataByteArrayChannel("abc".getBytes(Charsets.UTF_8), 0, 3);
    MockBlockWriter blockWriter = new MockBlockWriter();
    when(mBlockWorker.getTempBlockWriterRemote(sessionId, blockId)).thenReturn(blockWriter);
    RPCResponse response =
        request(new RPCBlockWriteRequest(sessionId, blockId, offset, length, data));

    // Verify that the write request requests space on an existing block and then writes the
    // specified data.
    assertEquals(RPCResponse.Status.SUCCESS, response.getStatus());
    verify(mBlockWorker).requestSpace(sessionId, blockId, length);
    assertEquals("ab", new String(blockWriter.getBytes(), Charsets.UTF_8));
  }

  @Test
  public void blockWorkerExceptionCausesFailStatusOnWrite() throws Exception {
    long sessionId = 0;
    long blockId = 1;
    long offset = 0;
    long length = 2;
    when(mBlockWorker.getTempBlockWriterRemote(sessionId, blockId))
        .thenThrow(new RuntimeException());
    DataByteArrayChannel data = new DataByteArrayChannel("abc".getBytes(Charsets.UTF_8), 0, 3);
    RPCResponse response =
        request(new RPCBlockWriteRequest(sessionId, blockId, offset, length, data));

    // Verify that the write request failed.
    assertEquals(RPCResponse.Status.WRITE_ERROR, response.getStatus());
  }

  @Test
  public void readFile() throws Exception {
    long tempUfsFileId = 1;
    long offset = 0;
    long length = 3;
    when(mFileSystemWorker.getUfsInputStream(tempUfsFileId, offset)).thenReturn(
        new ByteArrayInputStream("abc".getBytes(Charsets.UTF_8)));
    RPCResponse response = request(new RPCFileReadRequest(tempUfsFileId, offset, length));

    // Verify that the 3 bytes were read.
    assertEquals("abc",
        Charsets.UTF_8.decode(response.getPayloadDataBuffer().getReadOnlyByteBuffer()).toString());
  }

  @Test
  public void fileSystemWorkerExceptionCausesFailStatusOnRead() throws Exception {
    when(mFileSystemWorker.getUfsInputStream(1, 0))
        .thenThrow(new RuntimeException());
    RPCResponse response = request(new RPCFileReadRequest(1, 0, 3));

    // Verify that the write request failed.
    assertEquals(RPCResponse.Status.UFS_READ_FAILED, response.getStatus());
  }

  @Test
  public void writeFile() throws Exception {
    long tempUfsFileId = 1;
    long offset = 0;
    long length = 3;
    DataByteArrayChannel data = new DataByteArrayChannel("abc".getBytes(Charsets.UTF_8), 0, 3);
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    when(mFileSystemWorker.getUfsOutputStream(tempUfsFileId)).thenReturn(outStream);
    RPCResponse response = request(new RPCFileWriteRequest(tempUfsFileId, offset, length, data));

    // Verify that the write request writes to the OutputStream returned by the worker.
    assertEquals(RPCResponse.Status.SUCCESS, response.getStatus());
    assertEquals("abc", new String(outStream.toByteArray(), Charsets.UTF_8));
  }

  @Test
  public void fileSystemWorkerExceptionCausesFailStatusOnWrite() throws Exception {
    DataByteArrayChannel data = new DataByteArrayChannel("abc".getBytes(Charsets.UTF_8), 0, 3);
    when(mFileSystemWorker.getUfsOutputStream(1)).thenThrow(new RuntimeException());
    RPCResponse response = request(new RPCFileWriteRequest(1, 0, 3, data));

    // Verify that the write request failed.
    assertEquals(RPCResponse.Status.UFS_WRITE_FAILED, response.getStatus());
  }

  private RPCResponse request(RPCRequest rpcBlockWriteRequest) throws Exception {
    InetSocketAddress address =
        new InetSocketAddress(mNettyDataServer.getBindHost(), mNettyDataServer.getPort());
    ClientHandler handler = new ClientHandler();
    Bootstrap clientBootstrap = NettyClient.createClientBootstrap(handler);
    ChannelFuture f = clientBootstrap.connect(address).sync();
    Channel channel = f.channel();
    try {
      SingleResponseListener listener = new SingleResponseListener();
      handler.addListener(listener);
      channel.writeAndFlush(rpcBlockWriteRequest);
      return listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } finally {
      channel.close().sync();
    }
  }
}
