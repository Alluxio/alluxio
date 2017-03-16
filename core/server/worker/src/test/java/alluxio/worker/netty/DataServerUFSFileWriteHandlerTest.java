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
import alluxio.util.io.BufferUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.worker.file.FileSystemWorker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Random;

/**
 * Unit tests for {@link DataServerUFSFileWriteHandler}.
 */
public final class DataServerUFSFileWriteHandlerTest extends DataServerWriteHandlerTest {
  private final Random mRandom = new Random();

  private FileSystemWorker mFileSystemWorker;
  private OutputStream mOutputStream;

  @Before
  public void before() throws Exception {
    mFileSystemWorker = Mockito.mock(FileSystemWorker.class);
    mFile = mTestFolder.newFile().getPath();
    mOutputStream = new FileOutputStream(mFile);
    Mockito.when(mFileSystemWorker.getUfsOutputStream(Mockito.anyLong()))
        .thenReturn(mOutputStream);
    mChecksum = 0;
    mChannel = new EmbeddedChannel(
        new DataServerUFSFileWriteHandler(NettyExecutors.FILE_WRITER_EXECUTOR, mFileSystemWorker));
    mChannelNoException = new EmbeddedNoExceptionChannel(
        new DataServerUFSFileWriteHandler(NettyExecutors.FILE_WRITER_EXECUTOR, mFileSystemWorker));
  }

  @After
  public void after() throws Exception {
    mOutputStream.close();
  }

  /**
   * Tests write failure.
   */
  @Test
  public void writeFailure() throws Exception {
    mChannelNoException.writeInbound(buildWriteRequest(0, PACKET_SIZE));
    mOutputStream.close();
    mChannelNoException.writeInbound(buildWriteRequest(PACKET_SIZE, PACKET_SIZE));
    Object writeResponse = waitForResponse(mChannelNoException);
    checkWriteResponse(writeResponse, Protocol.Status.Code.INTERNAL);
  }

  @Override
  protected RPCProtoMessage buildWriteRequest(long offset, int len) {
    Protocol.WriteRequest writeRequest =
        Protocol.WriteRequest.newBuilder().setId(1L).setOffset(offset)
            .setType(Protocol.RequestType.UFS_FILE).build();
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
