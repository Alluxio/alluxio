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

package alluxio.worker.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;

import alluxio.Constants;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.Chunk;
import alluxio.grpc.RequestType;
import alluxio.grpc.WriteRequestCommand;
import alluxio.grpc.WriteResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteArrayChannel;
import alluxio.proto.status.Status.PStatus;
import alluxio.util.io.BufferUtils;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * Unit tests for {@link AbstractWriteHandler}.
 */
public abstract class AbstractWriteHandlerTest {
  private static final Random RANDOM = new Random();
  protected static final int PACKET_SIZE = 1024;
  protected static final long TEST_BLOCK_ID = 1L;
  protected static final long TEST_MOUNT_ID = 10L;
  protected AbstractWriteHandler mWriteHandler;
  protected StreamObserver<WriteResponse> mResponseObserver;
  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Test
  public void writeEmptyFile() throws Exception {
    mWriteHandler.write(newWriteRequestCommand(0));
    mWriteHandler.onComplete();
    checkComplete(mResponseObserver);
  }

  @Test
  public void writeNonEmptyFile() throws Exception {
    long len = 0;
    long checksum = 0;
    mWriteHandler.write(newWriteRequestCommand(0));
    for (int i = 0; i < 128; i++) {
      DataBuffer dataBuffer = newDataBuffer(PACKET_SIZE);
      checksum += getChecksum(dataBuffer);
      mWriteHandler.write(newWriteRequest(dataBuffer));
      len += PACKET_SIZE;
    }
    // EOF.
    mWriteHandler.onComplete();
    checkComplete(mResponseObserver);
    checkWriteData(checksum, len);
  }

  @Test
  public void cancel() throws Exception {
    long len = 0;
    long checksum = 0;
    mWriteHandler.write(newWriteRequestCommand(0));
    for (int i = 0; i < 1; i++) {
      DataBuffer dataBuffer = newDataBuffer(PACKET_SIZE);
      checksum += getChecksum(dataBuffer);
      mWriteHandler.write(newWriteRequest(dataBuffer));
      len += PACKET_SIZE;
    }
    // Cancel.
    mWriteHandler.onCancel();

    checkComplete(mResponseObserver);
    // Our current implementation does not really abort the file when the write is cancelled.
    // The client issues another request to block worker to abort it.
    checkWriteData(checksum, len);
  }

  @Test
  public void writeInvalidOffsetFirstRequest() throws Exception {
    // The write request contains an invalid offset
    mExpectedException.expect(InvalidArgumentException.class);
    mWriteHandler.write(newWriteRequestCommand(1));
  }

  @Test
  public void writeInvalidOffsetLaterRequest() throws Exception {
    mWriteHandler.write(newWriteRequestCommand(0));
    // The write request contains an invalid offset
    mExpectedException.expect(InvalidArgumentException.class);
    mWriteHandler.write(newWriteRequestCommand(1));
  }

  @Test
  public void writeTwoRequests() throws Exception {
    // Send first request
    DataBuffer dataBuffer = newDataBuffer(PACKET_SIZE);
    mWriteHandler.write(newWriteRequestCommand(0));
    mWriteHandler.write(newWriteRequest(dataBuffer));
    mWriteHandler.onComplete();
    // Wait the first packet to finish
    checkComplete(mResponseObserver);
    // Send second request
    mExpectedException.expect(InvalidArgumentException.class);
    mWriteHandler.write(newWriteRequestCommand(0));
    mWriteHandler.write(newWriteRequest(dataBuffer));
    mWriteHandler.onComplete();
  }

  @Test
  public void writeCancelAndRequests() throws Exception {
    // Send first request
    mWriteHandler.write(newWriteRequestCommand(0));
    mWriteHandler.onCancel();
    // Wait the first packet to finish
    checkComplete(mResponseObserver);
    // Send second request
    mExpectedException.expect(InvalidArgumentException.class);
    mWriteHandler.write(newWriteRequestCommand(0));
    mWriteHandler.onComplete();
  }

  @Test
  public void ErrorReceived() throws Exception {
    mWriteHandler.onError(new IOException("test exception"));
  }

  @Test
  public void ErrorReceivedAfterRequest() throws Exception {
    mWriteHandler.write(newWriteRequestCommand(0));
    mWriteHandler.onComplete();
    mWriteHandler.onError(new IOException("test exception"));
  }

  /**
   * Checks the given write response is expected and matches the given error code.
   *
   * @param expectedStatus the expected status code
   * @param writeResponse the write response
   */
  protected void checkWriteResponse(PStatus expectedStatus, WriteResponse writeResponse) {
    assertTrue(writeResponse.hasOffset());
    assertTrue(writeResponse.getOffset() > 0);
  }

  /**
   * Checks the file content matches expectation (file length and file checksum).
   *
   * @param expectedChecksum the expected checksum of the file
   * @param size the file size in bytes
   */
  protected void checkWriteData(long expectedChecksum, long size) throws IOException {
    long actualChecksum = 0;
    long actualSize = 0;

    byte[] buffer = new byte[(int) Math.min(Constants.KB, size)];
    try (InputStream input = getWriteDataStream()) {
      int bytesRead;
      while ((bytesRead = input.read(buffer)) >= 0) {
        for (int i = 0; i < bytesRead; i++) {
          actualChecksum += BufferUtils.byteToInt(buffer[i]);
          actualSize++;
        }
      }
    }

    assertEquals(expectedChecksum, actualChecksum);
    assertEquals(size, actualSize);
  }

  /**
   * Waits for a response.
   *
   * @return the response
   */
  protected WriteResponse waitForResponse(final StreamObserver<WriteResponse> responseObserver)
      throws TimeoutException, InterruptedException {
    ArgumentCaptor<WriteResponse> captor = ArgumentCaptor.forClass(WriteResponse.class);
    verify(responseObserver).onNext(captor.capture());
    return captor.getValue();
  }

  /**
   * Waits for an error.
   *
   * @return the error
   */
  protected Throwable getError(final StreamObserver<WriteResponse> responseObserver) {
    ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
    verify(responseObserver).onError(captor.capture());
    return captor.getValue();
  }

  /**
   * Checks that the response is completed.
   */
  protected void checkComplete(final StreamObserver<WriteResponse> responseObserver) {
    ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
    verify(responseObserver).onCompleted();
  }

  protected alluxio.grpc.WriteRequest newWriteRequestCommand(long offset) {
    return alluxio.grpc.WriteRequest.newBuilder().setCommand(
        WriteRequestCommand.newBuilder().setId(TEST_BLOCK_ID).setOffset(offset)
        .setType(getWriteRequestType())).build();
  }

  /**
   * Builds the write request with data.
   *
   * @param buffer the data to write
   * @return the write request
   */
  protected alluxio.grpc.WriteRequest newWriteRequest(DataBuffer buffer) {
    alluxio.grpc.WriteRequest writeRequest = alluxio.grpc.WriteRequest.newBuilder().setChunk(
        Chunk.newBuilder().setData(ByteString.copyFrom(buffer.getReadOnlyByteBuffer()))).build();
    return writeRequest;
  }

  /**
   * @return the write type of the request
   */
  protected abstract RequestType getWriteRequestType();

  /**
   * @return the data written as an InputStream
   */
  protected abstract InputStream getWriteDataStream() throws IOException;

  /**
   * @param len length of the data buffer
   * @return a newly created data buffer
   */
  protected DataBuffer newDataBuffer(int len) {
    byte[] buf = new byte[len];
    for (int i = 0; i < len; i++) {
      byte value = (byte) (RANDOM.nextInt() % Byte.MAX_VALUE);
      buf[i] = value;
    }
    return new DataByteArrayChannel(buf, 0, len);
  }

  /**
   * @param buffer buffer to get checksum
   * @return the checksum
   */
  public static long getChecksum(DataBuffer buffer) {
    return getChecksum((ByteBuf) buffer.getNettyOutput());
  }

  /**
   * @param buffer buffer to get checksum
   * @return the checksum
   */
  public static long getChecksum(ByteBuf buffer) {
    long ret = 0;
    for (int i = 0; i < buffer.capacity(); i++) {
      ret += BufferUtils.byteToInt(buffer.getByte(i));
    }
    return ret;
  }
}
