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
import alluxio.EmbeddedChannelNoException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.LocalFileBlockWriter;

import com.google.common.base.Function;
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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockWorker.class})
public final class DataServerBlockWriteHandlerTest {
  private static final int PACKET_SIZE = 1024;
  private final Random mRandom = new Random();
  private final long mBlockId = 1L;

  private BlockWorker mBlockWorker;
  private BlockWriter mBlockWriter;
  private String mFile;
  private long mChecksum;
  private EmbeddedChannel mChannel;
  private EmbeddedChannel mChannelNoException;

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

    mChannel = new EmbeddedChannel(
        new DataServerBlockWriteHandler(NettyExecutors.BLOCK_WRITER_EXECUTOR, mBlockWorker));
    mChannelNoException = new EmbeddedChannelNoException(
        new DataServerBlockWriteHandler(NettyExecutors.BLOCK_WRITER_EXECUTOR, mBlockWorker));
  }

  @Test
  public void writeEmptyFile() throws Exception {
    mChannel.writeInbound(buildWriteRequest(0, 0));

    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(writeResponse, Protocol.Status.Code.OK);
  }

  @Test
  public void writeNonEmptyFile() throws Exception {
    long len = 0;
    for (int i = 0; i < 128; i++) {
      mChannel.writeInbound(buildWriteRequest(len, PACKET_SIZE));
      len += PACKET_SIZE;
    }
    // EOF.
    mChannel.writeInbound(buildWriteRequest(len, 0));

    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(writeResponse, Protocol.Status.Code.OK);
    checkFileContent(len);
  }

  @Test
  public void writeInvalidOffset() throws Exception {
    mChannelNoException.writeInbound(buildWriteRequest(0, PACKET_SIZE));
    mChannelNoException.writeInbound(buildWriteRequest(PACKET_SIZE + 1, PACKET_SIZE));
    Object writeResponse = waitForResponse(mChannelNoException);
    Assert.assertTrue(writeResponse instanceof RPCProtoMessage);
    checkWriteResponse(writeResponse, Protocol.Status.Code.INVALID_ARGUMENT);
  }

  @Test
  public void writeFailure() throws Exception {
    mChannelNoException.writeInbound(buildWriteRequest(0, PACKET_SIZE));
    mBlockWriter.close();
    mChannelNoException.writeInbound(buildWriteRequest(PACKET_SIZE, PACKET_SIZE));
    Object writeResponse = waitForResponse(mChannelNoException);
    Assert.assertTrue(writeResponse instanceof RPCProtoMessage);
    checkWriteResponse(writeResponse, Protocol.Status.Code.INTERNAL);
  }

  /**
   * Builds the write request.
   *
   * @param offset the offset
   * @param len the length of the block
   * @return the write request
   */
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

  /**
   * Checks the given write response is expected and matches the given error code.
   *
   * @param writeResponse the write response
   * @param codeExpected the expected error code
   */
  private void checkWriteResponse(Object writeResponse, Protocol.Status.Code codeExpected) {
    Assert.assertTrue(writeResponse instanceof RPCProtoMessage);

    Object response = ((RPCProtoMessage) writeResponse).getMessage();
    Assert.assertTrue(response instanceof Protocol.Response);
    Assert.assertEquals(codeExpected, ((Protocol.Response) response).getStatus().getCode());
  }

  /**
   * Checks the file content matches expectation (file length and file checksum).
   *
   * @param size the file size in bytes
   * @throws IOException if it fails to check the file content
   */
  private void checkFileContent(long size) throws IOException {
    RandomAccessFile file = new RandomAccessFile(mFile, "r");
    long checksumActual = 0;
    long sizeActual = 0;

    byte[] buffer = new byte[(int) Math.min(Constants.KB, size)];
    int bytesRead;
    do {
      bytesRead = file.read(buffer);
      for (int i = 0; i < bytesRead; i++) {
        checksumActual += BufferUtils.byteToInt(buffer[i]);
        sizeActual++;
      }
    } while (bytesRead >= 0);

    Assert.assertEquals(mChecksum, checksumActual);
    Assert.assertEquals(size, sizeActual);
  }

  /**
   * Waits for a response.
   *
   * @return the response
   */
  private Object waitForResponse(final EmbeddedChannel channel) {
    return CommonUtils.waitFor("", new Function<Void, Object>() {
      @Override
      public Object apply(Void v) {
        return channel.readOutbound();
      }
    }, Constants.MINUTE_MS);
  }
}
