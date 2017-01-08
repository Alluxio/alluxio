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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.EmbeddedChannelNoException;
import alluxio.PropertyKey;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.google.common.base.Function;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
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
import java.nio.ByteBuffer;
import java.util.Random;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockWorker.class})
public final class DataServerBlockReadHandlerTest {
  private static final long PACKET_SIZE =
      Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES);
  private final Random mRandom = new Random();
  private final long mBlockId = 1L;

  private BlockWorker mBlockWorker;
  private BlockReader mBlockReader;
  private String mFile;
  private EmbeddedChannel mChannel;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
    mBlockWorker = PowerMockito.mock(BlockWorker.class);
    PowerMockito.doNothing().when(mBlockWorker).accessBlock(Mockito.anyLong(), Mockito.anyLong());
    mChannel = new EmbeddedChannel(
        new DataServerBlockReadHandler(NettyExecutors.BLOCK_READER_EXECUTOR, mBlockWorker,
            FileTransferType.MAPPED));
  }

  @Test
  public void readFullFile() throws Exception {
    long checksumExpected = populateInputFile(PACKET_SIZE * 10, 0, PACKET_SIZE * 10 - 1);
    mChannel.writeInbound(buildReadRequest(0, PACKET_SIZE * 10));
    checkAllReadResponses(checksumExpected);
  }

  @Test
  public void readPartialFile() throws Exception {
    long start = 3;
    long end = PACKET_SIZE * 10 - 99;
    long checksumExpected = populateInputFile(PACKET_SIZE * 10, start, end);
    mChannel.writeInbound(buildReadRequest(start, end + 1 - start));
    checkAllReadResponses(checksumExpected);
  }

  @Test
  public void reuseChannel() throws Exception {
    long fileSize = PACKET_SIZE * 5;
    long checksumExpected = populateInputFile(fileSize, 0, fileSize - 1);
    mChannel.writeInbound(buildReadRequest(0, fileSize));
    checkAllReadResponses(checksumExpected);

    fileSize = fileSize / 2 + 1;
    long start = 3;
    long end = fileSize - 1;
    checksumExpected = populateInputFile(fileSize, start, end);
    mChannel.writeInbound(buildReadRequest(start, end - start + 1));
    checkAllReadResponses(checksumExpected);
  }

  @Test
  public void transferType() throws Exception {
    mChannel = new EmbeddedChannel(
        new DataServerBlockReadHandler(NettyExecutors.BLOCK_READER_EXECUTOR, mBlockWorker,
            FileTransferType.TRANSFER));

    long fileSize = PACKET_SIZE * 2;
    long checksumExpected = populateInputFile(fileSize, 0, fileSize - 1);

    BlockReader blockReader = PowerMockito.spy(mBlockReader);
    // Do not call close here so that we can check result. It will be closed explicitly.
    PowerMockito.doNothing().when(blockReader).close();
    PowerMockito
        .when(mBlockWorker.readBlockRemote(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong()))
        .thenReturn(blockReader);
    mChannel.writeInbound(buildReadRequest(0, fileSize));
    checkAllReadResponses(checksumExpected);
    mBlockReader.close();
  }

  @Test
  public void readEmptyFile() throws Exception {
    mChannel = new EmbeddedChannelNoException(
        new DataServerBlockReadHandler(NettyExecutors.BLOCK_READER_EXECUTOR, mBlockWorker,
            FileTransferType.MAPPED));
    populateInputFile(0, 0, 0);
    mChannel.writeInbound(buildReadRequest(0, 0));
    Object response = waitForOneResponse();
    checkReadResponse(response, Protocol.Status.Code.INVALID_ARGUMENT);
  }

  @Test
  public void cancelRequest() throws Exception {
    long fileSize = PACKET_SIZE * 10 + 1;
    populateInputFile(fileSize, 0, fileSize - 1);
    RPCProtoMessage readRequest = buildReadRequest(0, fileSize);
    Protocol.ReadRequest request = (Protocol.ReadRequest) readRequest.getMessage();
    RPCProtoMessage cancelRequest =
        new RPCProtoMessage(request.toBuilder().setCancel(true).build(), null);
    mChannel.writeInbound(readRequest);
    mChannel.writeInbound(cancelRequest);

    // Make sure we can still get EOF after cancelling though the read request is not necessarily
    // fulfilled.
    boolean eof = false;
    long maxIterations = 100;
    while (maxIterations > 0) {
      Object response = waitForOneResponse();
      DataBuffer buffer = checkReadResponse(response, Protocol.Status.Code.OK);
      if (buffer == null) {
        eof = true;
        break;
      }
      buffer.release();
      maxIterations--;
      Assert.assertTrue(mChannel.isOpen());
    }
    Assert.assertTrue(eof);
  }

  @Test
  public void readFailure() throws Exception {
    mChannel = new EmbeddedChannelNoException(
        new DataServerBlockReadHandler(NettyExecutors.BLOCK_READER_EXECUTOR, mBlockWorker,
            FileTransferType.MAPPED));
    long fileSize = PACKET_SIZE * 10 + 1;
    populateInputFile(0, 0, fileSize - 1);
    mBlockReader.close();
    mChannel.writeInbound(buildReadRequest(0, fileSize));
    Object response = waitForOneResponse();
    checkReadResponse(response, Protocol.Status.Code.INTERNAL);
  }

  /**
   * Populates the input file, also computes the checksum for part of the file.
   *
   * @param length the length of the file
   * @param start the start position to compute the checksum
   * @param end the last position to compute the checksum
   * @throws Exception if it fails to populate the input file
   * @return the checksum
   */
  private long populateInputFile(long length, long start, long end) throws Exception {
    long checksum = 0;
    File file = mTestFolder.newFile();
    long pos = 0;
    if (length > 0) {
      FileOutputStream fileOutputStream = new FileOutputStream(file);
      while (length > 0) {
        byte[] buffer = new byte[(int) Math.min(length, Constants.MB)];
        mRandom.nextBytes(buffer);
        for (int i = 0; i < buffer.length; i++) {
          if (pos >= start && pos <= end) {
            checksum += BufferUtils.byteToInt(buffer[i]);
          }
          pos++;
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
    return checksum;
  }

  /**
   * Builds a read request.
   *
   * @param offset the offset
   * @param len the length to read
   * @return the proto message
   */
  private RPCProtoMessage buildReadRequest(long offset, long len) {
    Protocol.ReadRequest readRequest =
        Protocol.ReadRequest.newBuilder().setId(mBlockId).setOffset(offset).setLength(len)
            .setLockId(1L).setType(Protocol.RequestType.ALLUXIO_BLOCK).build();
    return new RPCProtoMessage(readRequest, null);
  }

  /**
   * Checks all the read responses.
   */
  private void checkAllReadResponses(long checksumExpected) {
    boolean eof = false;
    long checksumActual = 0;
    while (!eof) {
      Object readResponse = waitForOneResponse();
      if (readResponse == null) {
        Assert.fail();
        break;
      }
      DataBuffer buffer = checkReadResponse(readResponse, Protocol.Status.Code.OK);
      eof = buffer == null;
      if (buffer != null) {
        if (buffer instanceof DataNettyBufferV2) {
          ByteBuf buf = (ByteBuf) buffer.getNettyOutput();
          while (buf.readableBytes() > 0) {
            checksumActual += BufferUtils.byteToInt(buf.readByte());
          }
          buf.release();
        } else {
          Assert.assertTrue(buffer instanceof DataFileChannel);
          ByteBuffer buf = buffer.getReadOnlyByteBuffer();
          byte[] array = new byte[buf.remaining()];
          buf.get(array);
          for (int i = 0; i < array.length; i++) {
            checksumActual += BufferUtils.byteToInt(array[i]);
          }
        }
      }
    }
    Assert.assertEquals(checksumExpected, checksumActual);
    Assert.assertTrue(eof);
  }

  /**
   * Checks the read response message given the expected error code.
   *
   * @param readResponse the read response
   * @param codeExpected the expected error code
   * @return the data buffer extracted from the read response
   */
  private DataBuffer checkReadResponse(Object readResponse, Protocol.Status.Code codeExpected) {
    Assert.assertTrue(readResponse instanceof RPCProtoMessage);

    Object response = ((RPCProtoMessage) readResponse).getMessage();
    Assert.assertTrue(response instanceof Protocol.Response);
    Assert.assertEquals(codeExpected, ((Protocol.Response) response).getStatus().getCode());
    return ((RPCProtoMessage) readResponse).getPayloadDataBuffer();
  }

  /**
   * Waits for one read response messsage.
   *
   * @return the read response
   */
  private Object waitForOneResponse() {
    return CommonUtils.waitFor("", new Function<Void, Object>() {
      @Override
      public Object apply(Void v) {
        return mChannel.readOutbound();
      }
    }, Constants.MINUTE_MS);
  }
}
