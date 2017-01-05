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
import alluxio.worker.file.FileSystemWorker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Random;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemWorker.class})
public final class DataServerUFSFileWriteHandlerTest {
  private final Random mRandom = new Random();
  private final long mBlockId = 1L;

  private FileSystemWorker mFileSystemWorker;
  private OutputStream mOutputStream;
  private String mFile;
  private long mChecksum;
  private EmbeddedChannel mEmbeddedChannel;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mFileSystemWorker = PowerMockito.mock(FileSystemWorker.class);
    mFile = mTestFolder.newFile().getPath();
    mOutputStream = new FileOutputStream(mFile);
    PowerMockito.when(mFileSystemWorker.getUfsOutputStream(Mockito.anyLong()))
        .thenReturn(mOutputStream);
    mChecksum = 0;
    mEmbeddedChannel = new EmbeddedChannel(
        new DataServerUFSFileWriteHandler(NettyExecutors.FILE_WRITER_EXECUTOR, mFileSystemWorker));
  }

  @After
  public void after() throws Exception {
    mOutputStream.close();
  }

  @Test
  public void writeEmptyFile() throws Exception {
   mEmbeddedChannel.writeInbound(buildWriteRequest(0, 0));
    Object writeResponse = waitForResponse();
    checkWriteResponse(writeResponse, Protocol.Status.Code.OK);
  }

  @Test
  public void writeNonEmptyFile() throws Exception {
    long len = 0;
    for (int i = 0; i < 128; i++) {
      mEmbeddedChannel.writeInbound(buildWriteRequest(len, 1024));
      len += 1024;
    }
    // EOF.
    mEmbeddedChannel.writeInbound(buildWriteRequest(len, 0));

    Object writeResponse = waitForResponse();
    checkWriteResponse(writeResponse, Protocol.Status.Code.OK);
    checkFileContent(len);
  }

  @Test
  public void writeInvalidOffset() throws Exception {
    mEmbeddedChannel.writeInbound(buildWriteRequest(0, 1024));
    mEmbeddedChannel.writeInbound(buildWriteRequest(1025, 1024));
    Object writeResponse = waitForResponse();
    checkWriteResponse(writeResponse, Protocol.Status.Code.INVALID_ARGUMENT);
    waitForChannelClose();
    Assert.assertTrue(!mEmbeddedChannel.isOpen());
  }

  @Test
  public void writeFailure() throws Exception {
    mEmbeddedChannel.writeInbound(buildWriteRequest(0, 1024));
    mOutputStream.close();
    mEmbeddedChannel.writeInbound(buildWriteRequest(1024, 1024));
    Object writeResponse = waitForResponse();
    checkWriteResponse(writeResponse, Protocol.Status.Code.INTERNAL);
    waitForChannelClose();
    Assert.assertTrue(!mEmbeddedChannel.isOpen());
  }

  private RPCProtoMessage buildWriteRequest(long offset, int len) {
    Protocol.WriteRequest writeRequest =
        Protocol.WriteRequest.newBuilder().setId(mBlockId).setOffset(offset).setSessionId(-1L)
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

  private Object waitForResponse() {
    Object writeResponse = null;
    int timeRemaining = Constants.MINUTE_MS;
    while (writeResponse == null && timeRemaining > 0) {
      writeResponse = mEmbeddedChannel.readOutbound();
      CommonUtils.sleepMs(10);
      timeRemaining -= 10;
    }
    return writeResponse;
  }

  private void waitForChannelClose() {
    int timeRemaining = Constants.MINUTE_MS;
    while (timeRemaining > 0 && mEmbeddedChannel.isOpen()) {}
  }
}
