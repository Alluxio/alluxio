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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import alluxio.Constants;
import alluxio.grpc.Chunk;
import alluxio.grpc.RequestType;
import alluxio.grpc.WriteRequestCommand;
import alluxio.grpc.WriteResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.ByteArrayDataBuffer;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * Unit tests for {@link AbstractWriteHandler}.
 */
public abstract class AbstractWriteHandlerTest {
  private static final Random RANDOM = new Random();
  protected static final int CHUNK_SIZE = 1024;
  protected static final long TEST_BLOCK_ID = 1L;
  protected static final long TEST_MOUNT_ID = 10L;
  protected AbstractWriteHandler mWriteHandler;
  protected StreamObserver<WriteResponse> mResponseObserver;
  protected AuthenticatedUserInfo mUserInfo = new AuthenticatedUserInfo();
  protected List<WriteResponse> mResponses = new ArrayList<>();
  protected boolean mResponseCompleted;
  protected Throwable mError;

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Test
  public void writeEmptyFile() throws Exception {
    mWriteHandler.write(newWriteRequestCommand(0));
    mWriteHandler.onCompleted();
    waitForResponses();
    checkComplete(mResponseObserver);
  }

  @Test
  public void writeNonEmptyFile() throws Exception {
    long len = 0;
    long checksum = 0;
    mWriteHandler.write(newWriteRequestCommand(0));
    for (int i = 0; i < 128; i++) {
      DataBuffer dataBuffer = newDataBuffer(CHUNK_SIZE);
      checksum += getChecksum(dataBuffer);
      mWriteHandler.write(newWriteRequest(dataBuffer));
      len += CHUNK_SIZE;
    }
    // EOF.
    mWriteHandler.onCompleted();
    waitForResponses();
    checkComplete(mResponseObserver);
    checkWriteData(checksum, len);
  }

  @Test
  public void cancel() throws Exception {
    long len = 0;
    long checksum = 0;
    mWriteHandler.write(newWriteRequestCommand(0));
    for (int i = 0; i < 1; i++) {
      DataBuffer dataBuffer = newDataBuffer(CHUNK_SIZE);
      checksum += getChecksum(dataBuffer);
      mWriteHandler.write(newWriteRequest(dataBuffer));
      len += CHUNK_SIZE;
    }
    // Cancel.
    mWriteHandler.onCancel();

    waitForResponses();
    checkComplete(mResponseObserver);
    // Our current implementation does not really abort the file when the write is cancelled.
    // The client issues another request to block worker to abort it.
    checkWriteData(checksum, len);
  }

  @Test
  public void cancelIgnoreError() throws Exception {
    long len = 0;
    long checksum = 0;
    mWriteHandler.write(newWriteRequestCommand(0));
    for (int i = 0; i < 1; i++) {
      DataBuffer dataBuffer = newDataBuffer(CHUNK_SIZE);
      checksum += getChecksum(dataBuffer);
      mWriteHandler.write(newWriteRequest(dataBuffer));
      len += CHUNK_SIZE;
    }
    // Cancel.
    mWriteHandler.onCancel();
    mWriteHandler.onError(Status.CANCELLED.asRuntimeException());

    waitForResponses();
    checkComplete(mResponseObserver);
    checkWriteData(checksum, len);
    verify(mResponseObserver, never()).onError(any(Throwable.class));
  }

  @Test
  public void writeInvalidOffsetFirstRequest() throws Exception {
    // The write request contains an invalid offset
    mWriteHandler.write(newWriteRequestCommand(1));
    waitForResponses();
    checkErrorCode(mResponseObserver, Status.Code.INVALID_ARGUMENT);
  }

  @Test
  public void writeInvalidOffsetLaterRequest() throws Exception {
    mWriteHandler.write(newWriteRequestCommand(0));
    // The write request contains an invalid offset
    mWriteHandler.write(newWriteRequestCommand(1));
    waitForResponses();
    checkErrorCode(mResponseObserver, Status.Code.INVALID_ARGUMENT);
  }

  @Test
  public void ErrorReceived() throws Exception {
    mWriteHandler.onError(new IOException("test exception"));
  }

  @Test
  public void ErrorReceivedAfterRequest() throws Exception {
    mWriteHandler.write(newWriteRequestCommand(0));
    mWriteHandler.onCompleted();
    mWriteHandler.onError(new IOException("test exception"));
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
   * Checks an error is returned with given code.
   *
   * @param responseObserver the response stream observer
   * @param code the expected error code
   */
  protected void checkErrorCode(final StreamObserver<WriteResponse> responseObserver,
      Status.Code code) {
    Throwable t = getError(responseObserver);
    assertTrue(t instanceof StatusException);
    assertEquals(code, ((StatusException) t).getStatus().getCode());
  }

  /**
   * Checks that the response is completed.
   */
  protected void checkComplete(final StreamObserver<WriteResponse> responseObserver) {
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
    return new ByteArrayDataBuffer(buf, 0, len);
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

  /**
   * Waits for response messages.
   */
  protected void waitForResponses()
      throws TimeoutException, InterruptedException {
    CommonUtils.waitFor("response", () -> mResponseCompleted || mError != null,
        WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
  }

  protected void setupResponseTrigger() {
    doAnswer(args -> {
      mResponseCompleted = true;
      return null;
    }).when(mResponseObserver).onCompleted();
    doAnswer(args -> {
      mResponseCompleted = true;
      mError = args.getArgument(0, Throwable.class);
      return null;
    }).when(mResponseObserver).onError(any(Throwable.class));
    doAnswer((args) -> {
      // make a copy of response data before it is released
      mResponses.add(WriteResponse.parseFrom(
          args.getArgument(0, WriteResponse.class).toByteString()));
      return null;
    }).when(mResponseObserver).onNext(any(WriteResponse.class));
  }
}
