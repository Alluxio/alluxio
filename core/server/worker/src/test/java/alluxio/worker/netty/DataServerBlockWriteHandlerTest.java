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

import alluxio.EmbeddedNoExceptionChannel;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.status.Status.PStatus;
import alluxio.util.io.BufferUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.LocalFileBlockWriter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Random;

/**
 * Unit tests for {@link DataServerBlockWriteHandler}.
 */
public final class DataServerBlockWriteHandlerTest extends DataServerWriteHandlerTest {
  private final Random mRandom = new Random();

  private BlockWorker mBlockWorker;
  private BlockWriter mBlockWriter;

  @Before
  public void before() throws Exception {
    mBlockWorker = Mockito.mock(BlockWorker.class);
    Mockito.doNothing().when(mBlockWorker)
        .createBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString(),
            Mockito.anyLong());
    Mockito.doNothing().when(mBlockWorker)
        .requestSpace(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
    Mockito.doNothing().when(mBlockWorker).abortBlock(Mockito.anyLong(), Mockito.anyLong());
    Mockito.doNothing().when(mBlockWorker).commitBlock(Mockito.anyLong(), Mockito.anyLong());
    mFile = mTestFolder.newFile().getPath();
    mBlockWriter = new LocalFileBlockWriter(mFile);
    Mockito.when(mBlockWorker.getTempBlockWriterRemote(Mockito.anyLong(), Mockito.anyLong()))
        .thenReturn(mBlockWriter);
    mChecksum = 0;

    mChannel = new EmbeddedChannel(
        new DataServerBlockWriteHandler(NettyExecutors.BLOCK_WRITER_EXECUTOR, mBlockWorker));
    mChannelNoException = new EmbeddedNoExceptionChannel(
        new DataServerBlockWriteHandler(NettyExecutors.BLOCK_WRITER_EXECUTOR, mBlockWorker));
  }

  /**
   * Tests write failure.
   */
  @Test
  public void writeFailure() throws Exception {
    mChannelNoException.writeInbound(buildWriteRequest(0, PACKET_SIZE));
    mBlockWriter.close();
    mChannelNoException.writeInbound(buildWriteRequest(PACKET_SIZE, PACKET_SIZE));
    Object writeResponse = waitForResponse(mChannelNoException);
    Assert.assertTrue(writeResponse instanceof RPCProtoMessage);
    checkWriteResponse(writeResponse, PStatus.FAILED_PRECONDITION);
  }

  @Override
  protected RPCProtoMessage buildWriteRequest(long offset, int len) {
    Protocol.WriteRequest writeRequest =
        Protocol.WriteRequest.newBuilder().setId(1L).setOffset(offset)
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
    if (len == EOF) {
      writeRequest = writeRequest.toBuilder().setEof(true).build();
    }
    if (len == CANCEL) {
      writeRequest = writeRequest.toBuilder().setCancel(true).build();
    }
    return new RPCProtoMessage(new ProtoMessage(writeRequest), buffer);
  }
}
