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
import alluxio.PropertyKey;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;
import alluxio.worker.file.FileSystemWorker;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Random;

@RunWith(PowerMockRunner.class)
public final class DataServerUFSFileReadHandlerTest {
  private static final long PACKET_SIZE =
      Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES);
  private final Random mRandom = new Random();
  private final long mBlockId = 1L;

  private FileSystemWorker mFileSystemWorker;
  private InputStream mInputStream;
  private String mFile;
  private long mChecksum;
  private EmbeddedChannel mChannel;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
    mFileSystemWorker = PowerMockito.mock(FileSystemWorker.class);
    mChannel = new EmbeddedChannel(
        new DataServerUFSFileReadHandler(NettyExecutors.FILE_READER_EXECUTOR, mFileSystemWorker));
  }

  @After
  public void after() throws Exception {
    mInputStream.close();
  }

  @Test
  public void readFullFile() throws Exception {
    populateInputFile(PACKET_SIZE * 10, 0, PACKET_SIZE * 10 - 1);
    mChannel.writeInbound(buildReadRequest(0, PACKET_SIZE * 10));
    checkAllReadResponses();
  }

  @Test
  public void readPartialFile() throws Exception {
    long start = 3;
    long end = PACKET_SIZE * 10 - 99;
    populateInputFile(PACKET_SIZE * 10, start, end);
    mChannel.writeInbound(buildReadRequest(start, end + 1 - start));
    checkAllReadResponses();
  }

  @Test
  public void reuseChannel() throws Exception {
    long fileSize = PACKET_SIZE * 5;
    populateInputFile(fileSize, 0, fileSize - 1);
    mChannel.writeInbound(buildReadRequest(0, fileSize));
    checkAllReadResponses();

    fileSize = fileSize / 2 + 1;
    long start = 3;
    long end = fileSize - 1;
    populateInputFile(fileSize, start, end);
    mChannel.writeInbound(buildReadRequest(start, end - start + 1));
    checkAllReadResponses();
  }

  @Test
  public void readEmptyFile() throws Exception {
    populateInputFile(0, 0, 0);
    mChannel.writeInbound(buildReadRequest(0, 0));
    Object response = waitForOneResponse();
    checkReadResponse(response, Protocol.Status.Code.INVALID_ARGUMENT);
    waitForChannelClose();
    Assert.assertTrue(!mChannel.isOpen());
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
    boolean EOF = false;
    long maxIterations = 100;
    while (maxIterations > 0) {
      Object response = waitForOneResponse();
      DataBuffer buffer = checkReadResponse(response, Protocol.Status.Code.OK);
      if (buffer == null) {
        EOF = true;
        break;
      }
      buffer.release();
      maxIterations--;
      Assert.assertTrue(mChannel.isOpen());
    }
    Assert.assertTrue(EOF);
  }

  @Test
  public void readFailure() throws Exception {
    long fileSize = PACKET_SIZE * 10 + 1;
    populateInputFile(0, 0, fileSize - 1);
    mInputStream.close();
    mChannel.writeInbound(buildReadRequest(0, fileSize));
    Object response = waitForOneResponse();
    checkReadResponse(response, Protocol.Status.Code.INTERNAL);
    waitForChannelClose();
    Assert.assertTrue(!mChannel.isOpen());
  }

  /**
   * Populates the input file, also computes the checksum for part of the file.
   *
   * @param length the length of the file
   * @param start the start position to compute the checksum
   * @param end the last position to compute the checksum
   * @throws Exception if it fails to populate the input file
   */
  private void populateInputFile(long length, long start, long end) throws Exception {
    mChecksum = 0;
    File file = mTestFolder.newFile();
    long pos = 0;
    if (length > 0) {
      FileOutputStream fileOutputStream = new FileOutputStream(file);
      while (length > 0) {
        byte[] buffer = new byte[(int) Math.min(length, Constants.MB)];
        mRandom.nextBytes(buffer);
        for (int i = 0; i < buffer.length; i++) {
          if (pos >= start && pos <= end) {
            mChecksum += BufferUtils.byteToInt(buffer[i]);
          }
          pos++;
        }
        fileOutputStream.write(buffer);
        length -= buffer.length;
      }
      fileOutputStream.close();
    }

    mFile = file.getPath();
    mInputStream = new FileInputStream(mFile);
    mInputStream.skip(start);
    PowerMockito.when(mFileSystemWorker.getUfsInputStream(Mockito.anyLong(), Mockito.anyLong()))
        .thenReturn(mInputStream);
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
        Protocol.ReadRequest.newBuilder().setId(mBlockId).setOffset(offset).setSessionId(1L)
            .setLength(len).setLockId(1L).setType(Protocol.RequestType.UFS_FILE).build();
    return new RPCProtoMessage(readRequest, null);
  }

  /**
   * Checks all the read responses.
   */
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
        Assert.assertTrue(buffer instanceof DataNettyBufferV2);
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
    Object writeResponse = null;
    int timeRemaining = Constants.MINUTE_MS;
    while (writeResponse == null && timeRemaining > 0) {
      writeResponse = mChannel.readOutbound();
      CommonUtils.sleepMs(10);
      timeRemaining -= 10;
    }
    return writeResponse;
  }

  /**
   * Waits for the channel to close.
   */
  private void waitForChannelClose() {
    int timeRemaining = Constants.MINUTE_MS;
    while (timeRemaining > 0 && mChannel.isOpen()) {
    }
  }
}
