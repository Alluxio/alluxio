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
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.util.proto.ProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Function;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

public abstract class DataServerReadHandlerTest {
  protected static final long PACKET_SIZE =
      Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES);
  private final Random mRandom = new Random();

  protected String mFile;
  protected EmbeddedChannel mChannel;
  protected EmbeddedChannel mChannelNoException;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Reads all bytes of a file.
   */
  @Test
  public void readFullFile() throws Exception {
    long checksumExpected = populateInputFile(PACKET_SIZE * 10, 0, PACKET_SIZE * 10 - 1);
    mChannel.writeInbound(buildReadRequest(0, PACKET_SIZE * 10));
    checkAllReadResponses(mChannel, checksumExpected);
  }

  /**
   * Reads a sub-region of a file.
   */
  @Test
  public void readPartialFile() throws Exception {
    long start = 3;
    long end = PACKET_SIZE * 10 - 99;
    long checksumExpected = populateInputFile(PACKET_SIZE * 10, start, end);
    mChannel.writeInbound(buildReadRequest(start, end + 1 - start));
    checkAllReadResponses(mChannel, checksumExpected);
  }

  /**
   * Handles multiple read requests within a channel sequentially.
   */
  @Test
  public void reuseChannel() throws Exception {
    long fileSize = PACKET_SIZE * 5;
    long checksumExpected = populateInputFile(fileSize, 0, fileSize - 1);
    mChannel.writeInbound(buildReadRequest(0, fileSize));
    checkAllReadResponses(mChannel, checksumExpected);

    fileSize = fileSize / 2 + 1;
    long start = 3;
    long end = fileSize - 1;
    checksumExpected = populateInputFile(fileSize, start, end);
    mChannel.writeInbound(buildReadRequest(start, end - start + 1));
    checkAllReadResponses(mChannel, checksumExpected);
  }

  /**
   * Fails if the read request tries to read an empty file.
   */
  @Test
  public void readEmptyFile() throws Exception {
    populateInputFile(0, 0, 0);
    mChannelNoException.writeInbound(buildReadRequest(0, 0));
    Object response = waitForOneResponse(mChannelNoException);
    checkReadResponse(response, Protocol.Status.Code.INVALID_ARGUMENT);
  }

  /**
   * Cancels the read request immediately after the read request is sent.
   */
  @Test
  public void cancelRequest() throws Exception {
    long fileSize = PACKET_SIZE * 100 + 1;
    populateInputFile(fileSize, 0, fileSize - 1);
    RPCProtoMessage readRequest = buildReadRequest(0, fileSize);
    Protocol.ReadRequest request = readRequest.getMessage().getMessage();
    RPCProtoMessage cancelRequest =
        new RPCProtoMessage(new ProtoMessage(request.toBuilder().setCancel(true).build()), null);
    mChannel.writeInbound(readRequest);
    mChannel.writeInbound(cancelRequest);

    // Make sure we can still get EOF after cancelling though the read request is not necessarily
    // fulfilled.
    boolean eof = false;
    long maxIterations = 100;
    while (maxIterations > 0) {
      Object response = waitForOneResponse(mChannel);
      // There is small chance that we can still receive an OK response here because it is too
      // fast to read all the data. If that ever happens, either increase the file size or allow it
      // to be OK here.
      DataBuffer buffer = checkReadResponse(response, Protocol.Status.Code.CANCELLED);
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

  /**
   * Populates the input file, also computes the checksum for part of the file.
   *
   * @param length the length of the file
   * @param start the start position to compute the checksum
   * @param end the last position to compute the checksum
   * @throws Exception if it fails to populate the input file
   * @return the checksum
   */
  protected long populateInputFile(long length, long start, long end) throws Exception {
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
    mockReader(start);
    return checksum;
  }

  /**
   * Checks all the read responses.
   */
  protected void checkAllReadResponses(EmbeddedChannel channel, long checksumExpected) {
    boolean eof = false;
    long checksumActual = 0;
    while (!eof) {
      Object readResponse = waitForOneResponse(channel);
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
  protected DataBuffer checkReadResponse(Object readResponse, Protocol.Status.Code codeExpected) {
    Assert.assertTrue(readResponse instanceof RPCProtoMessage);

    ProtoMessage response = ((RPCProtoMessage) readResponse).getMessage();
    Assert.assertTrue(response.getType() == ProtoMessage.Type.RESPONSE);
    DataBuffer buffer = ((RPCProtoMessage) readResponse).getPayloadDataBuffer();
    if (buffer != null) {
      Assert.assertEquals(Protocol.Status.Code.OK,
          response.<Protocol.Response>getMessage().getStatus().getCode());
    } else {
      Assert.assertEquals(codeExpected,
          response.<Protocol.Response>getMessage().getStatus().getCode());
    }
    return buffer;
  }

  /**
   * Waits for one read response messsage.
   *
   * @return the read response
   */
  protected Object waitForOneResponse(final EmbeddedChannel channel) {
    return CommonUtils.waitForResult("response from the channel", new Function<Void, Object>() {
      @Override
      public Object apply(Void v) {
        return channel.readOutbound();
      }
    }, WaitForOptions.defaults().setTimeout(Constants.MINUTE_MS));
  }

  /**
   * Builds a read request.
   *
   * @param offset the offset
   * @param len the length to read
   * @return the proto message
   */
  protected abstract RPCProtoMessage buildReadRequest(long offset, long len);

  /**
   * Mocks the reader (block reader or UFS file reader).
   *
   * @param start the start pos of the reader
   * @throws Exception if it fails to mock the reader
   */
  protected abstract void mockReader(long start) throws Exception;
}
