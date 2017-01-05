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

import alluxio.Constants;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockWorker.class})
public final class DataServerBlockReadHandlerTest {
  private final Random mRandom = new Random();
  private final long mBlockId = 1L;

  private BlockWorker mBlockWorker;
  private BlockReader mBlockReader;
  private String mFile;
  private long mChecksum;
  private EmbeddedChannel mChannel;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mBlockWorker = PowerMockito.mock(BlockWorker.class);
    PowerMockito.doNothing().when(mBlockWorker).accessBlock(Mockito.anyLong(), Mockito.anyLong());
    mChannel = new EmbeddedChannel(
        new DataServerBlockReadHandler(NettyExecutors.BLOCK_READER_EXECUTOR, mBlockWorker,
            FileTransferType.MAPPED));
  }

  @Test
  public void readEmptyFile() throws Exception {
    populateInputFile(1);
    mChannel.writeInbound(buildReadRequest(0, 1));
    mChannel.runPendingTasks();
    checkAllReadResponses();
  }

  /*
  @Test
  public void writeNonEmptyFile() throws Exception {
    final EmbeddedChannel channel = new EmbeddedChannel(
        new DataServerBlockWriteHandler(NettyExecutors.BLOCK_WRITER_EXECUTOR, mBlockWorker));
    long len = 0;
    for (int i = 0; i < 128; i++) {
      channel.writeInbound(buildWriteRequest(len, 1024));
      len += 1024;
    }
    // EOF.
    channel.writeInbound(buildWriteRequest(len, 0));

    Object writeResponse = waitForResponse(channel);
    checkWriteResponse(writeResponse, Protocol.Status.Code.OK);
    checkFileContent(len);
  }

  @Test
  public void writeInvalidOffset() throws Exception {
    final EmbeddedChannel channel = new EmbeddedChannel(
        new DataServerBlockWriteHandler(NettyExecutors.BLOCK_WRITER_EXECUTOR, mBlockWorker));
    channel.writeInbound(buildWriteRequest(0, 1024));
    channel.writeInbound(buildWriteRequest(1025, 1024));
    Object writeResponse = waitForResponse(channel);
    Assert.assertTrue(writeResponse instanceof RPCProtoMessage);
    checkWriteResponse(writeResponse, Protocol.Status.Code.INVALID_ARGUMENT);
    waitForChannelClose(channel);
    Assert.assertTrue(!channel.isOpen());
  }

  @Test
  public void writeFailure() throws Exception {
    final EmbeddedChannel channel = new EmbeddedChannel(
        new DataServerBlockWriteHandler(NettyExecutors.BLOCK_WRITER_EXECUTOR, mBlockWorker));
    channel.writeInbound(buildWriteRequest(0, 1024));
    mBlockWriter.close();
    channel.writeInbound(buildWriteRequest(1024, 1024));
    Object writeResponse = waitForResponse(channel);
    Assert.assertTrue(writeResponse instanceof RPCProtoMessage);
    checkWriteResponse(writeResponse, Protocol.Status.Code.INTERNAL);
    waitForChannelClose(channel);
    Assert.assertTrue(!channel.isOpen());
  }
  */

  private void populateInputFile(long length) throws Exception {
    File file = mTestFolder.newFile();
    if (length > 0) {
      FileOutputStream fileOutputStream = new FileOutputStream(file);
      while (length > 0) {
        byte[] buffer = new byte[(int) Math.min(length, Constants.MB)];
        mRandom.nextBytes(buffer);
        for (int i = 0; i < buffer.length; i++) {
          mChecksum += BufferUtils.byteToInt(buffer[i]);
        }
        fileOutputStream.write(buffer);
        length -= buffer.length;
      }
      fileOutputStream.close();
    }

    mFile = file.getPath();
    mBlockReader = new LocalFileBlockReader(mFile);
    PowerMockito
        .when(mBlockWorker.readBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong()))
        .thenReturn(mBlockReader);
  }

  private RPCProtoMessage buildReadRequest(long offset, long len) {
    Protocol.ReadRequest readRequest =
        Protocol.ReadRequest.newBuilder().setId(mBlockId).setOffset(offset).setSessionId(1L)
            .setLength(len).setLockId(1L).setType(Protocol.RequestType.ALLUXIO_BLOCK).build();
    return new RPCProtoMessage(readRequest, null);
  }

  private void checkAllReadResponses() {
    int timeRemaining = Constants.MINUTE_MS;
    boolean EOF = false;
    long checksumActual = 0;
    while (!EOF && timeRemaining > 0) {
      Object readResponse = null;
      while (readResponse == null && timeRemaining > 0) {
        readResponse = mChannel.readOutbound();
        CommonUtils.sleepMs(10);
        timeRemaining -= 10;
      }
      DataBuffer buffer = checkReadResponse(readResponse, Protocol.Status.Code.OK);
      EOF = buffer == null;
      if (buffer != null) {
        ByteBuf buf = (ByteBuf) buffer.getNettyOutput();
        while (buf.readableBytes() > 0) {
          checksumActual += BufferUtils.byteToInt(buf.readByte());
        }
        buf.release();
      }
    }
    Assert.assertEquals(mChecksum, checksumActual);
    Assert.assertTrue(EOF);
  }

  private DataBuffer checkReadResponse(Object readResponse, Protocol.Status.Code codeExpected) {
    Assert.assertTrue(readResponse instanceof RPCProtoMessage);

    Object response = ((RPCProtoMessage) readResponse).getMessage();
    Assert.assertTrue(response instanceof Protocol.Response);
    Assert.assertEquals(codeExpected, ((Protocol.Response) response).getStatus().getCode());
    return ((RPCProtoMessage) readResponse).getPayloadDataBuffer();
  }

  private void waitForChannelClose(EmbeddedChannel channel) {
    int timeRemaining = Constants.MINUTE_MS;
    while (timeRemaining > 0 && channel.isOpen()) {}
  }
}
