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
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Function;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.RandomAccessFile;

public abstract class DataServerWriteHandlerTest {
  protected static final int PACKET_SIZE = 1024;

  protected long mChecksum;
  protected EmbeddedChannel mChannel;
  protected EmbeddedChannel mChannelNoException;

  /** The file used to hold the data written by the test. */
  protected String mFile;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Writes an empty file.
   */
  @Test
  public void writeEmptyFile() throws Exception {
    mChannel.writeInbound(buildWriteRequest(0, 0));

    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(writeResponse, Protocol.Status.Code.OK);
  }

  /**
   * Writes an non-empty file.
   */
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

  /**
   * Fails if the write request contains an invalid offset.
   */
  @Test
  public void writeInvalidOffset() throws Exception {
    mChannelNoException.writeInbound(buildWriteRequest(0, PACKET_SIZE));
    mChannelNoException.writeInbound(buildWriteRequest(PACKET_SIZE + 1, PACKET_SIZE));
    Object writeResponse = waitForResponse(mChannelNoException);
    Assert.assertTrue(writeResponse instanceof RPCProtoMessage);
    checkWriteResponse(writeResponse, Protocol.Status.Code.INVALID_ARGUMENT);
  }

  /**
   * Checks the given write response is expected and matches the given error code.
   *
   * @param writeResponse the write response
   * @param codeExpected the expected error code
   */
  protected void checkWriteResponse(Object writeResponse, Protocol.Status.Code codeExpected) {
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
  protected void checkFileContent(long size) throws IOException {
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
  protected Object waitForResponse(final EmbeddedChannel channel) {
    return CommonUtils
        .waitForResult("response from the channel.", new Function<Void, Object>() {
          @Override
          public Object apply(Void v) {
            return channel.readOutbound();
          }
        }, WaitForOptions.defaults().setTimeout(Constants.MINUTE_MS));
  }

  /**
   * Builds the write request.
   *
   * @param offset the offset
   * @param len the length of the block
   * @return the write request
   */
  protected abstract RPCProtoMessage buildWriteRequest(long offset, int len);
}
