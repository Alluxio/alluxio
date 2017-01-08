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
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.LocalFileBlockWriter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
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

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockWorker.class})
public final class DataServerBlockWriteHandlerTest {
  private final Random mRandom = new Random();
  private final long mBlockId = 1L;

  private BlockWorker mBlockWorker;
  private BlockWriter mBlockWriter;
  private String mFile;
  private long mChecksum;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mBlockWorker = PowerMockito.mock(BlockWorker.class);
    PowerMockito.doNothing().when(mBlockWorker)
        .createBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString(),
            Mockito.anyLong());
    PowerMockito.doNothing().when(mBlockWorker)
        .requestSpace(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
    mFile = mTestFolder.newFile().getPath();
    mBlockWriter = new LocalFileBlockWriter(mFile);
    PowerMockito.when(mBlockWorker.getTempBlockWriterRemote(Mockito.anyLong(), Mockito.anyLong()))
        .thenReturn(mBlockWriter);
    mChecksum = 0;
  }

  @Test
  public void writeEmptyFile() throws Exception {
    final EmbeddedChannel channel = new EmbeddedChannel(
        new DataServerBlockWriteHandler(NettyExecutors.BLOCK_WRITER_EXECUTOR, mBlockWorker));
   channel.writeInbound(buildWriteRequest(0, 0));

    Object writeResponse = null;
    while (writeResponse == null) {
      writeResponse = channel.readOutbound();
      CommonUtils.sleepMs(10);
    }
    checkWriteResponse(writeResponse, Protocol.Status.Code.OK);
  }

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

  private RPCProtoMessage buildWriteRequest(long offset, int len) {
    Protocol.WriteRequest writeRequest =
        Protocol.WriteRequest.newBuilder().setId(mBlockId).setOffset(offset).setSessionId(1L)
            .setType(Protocol.RequestType.ALLUXIO_BLOCK).build();
    DataBuffer buffer = null;
    if (len > 0) {
      ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(len);
      for (int i = 0; i < len; i++) {
        byte value = (byte) (mRandom.nextInt() % Byte.MAX_VALUE);
        buf.writeByte(value);
        mChecksum += BufferUtils.byteToInt(value);
      }
      buffer = new DataNettyBufferV2(buf);
    }
    return new RPCProtoMessage(writeRequest, buffer);
  }

  private void checkWriteResponse(Object writeResponse, Protocol.Status.Code codeExpected) {
    Assert.assertTrue(writeResponse instanceof RPCProtoMessage);

    Object response = ((RPCProtoMessage) writeResponse).getMessage();
    Assert.assertTrue(response instanceof Protocol.Response);
    Assert.assertEquals(codeExpected, ((Protocol.Response) response).getStatus().getCode());
  }

  private void checkFileContent(long size) throws IOException {
    RandomAccessFile file = new RandomAccessFile(mFile, "r");
    long checksumActual = 0;
    long sizeActual = 0;
    try {
      while (true) {
        checksumActual += BufferUtils.byteToInt(file.readByte());
        sizeActual++;
      }
    } catch (EOFException e) {
      // expected.
    }
    Assert.assertEquals(mChecksum, checksumActual);
    Assert.assertEquals(size, sizeActual);
  }

  private Object waitForResponse(EmbeddedChannel channel) {
    Object writeResponse = null;
    int timeRemaining = Constants.MINUTE_MS;
    while (writeResponse == null && timeRemaining > 0) {
      writeResponse = channel.readOutbound();
      CommonUtils.sleepMs(10);
      timeRemaining -= 10;
    }
    return writeResponse;
  }

  private void waitForChannelClose(EmbeddedChannel channel) {
    int timeRemaining = Constants.MINUTE_MS;
    while (timeRemaining > 0 && channel.isOpen()) {}
  }
}
