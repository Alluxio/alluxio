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

import static org.mockito.Mockito.verify;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;

import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public abstract class ReadHandlerTest {
  protected static final long CHUNK_SIZE =
      Configuration.getBytes(PropertyKey.USER_NETWORK_READER_CHUNK_SIZE_BYTES);
  private final Random mRandom = new Random();

  protected String mFile;
  protected AbstractReadHandler mReadHandlerNoException;
  protected AbstractReadHandler mReadHandler;
  protected ServerCallStreamObserver<ReadResponse> mResponseObserver;
  protected boolean mResponseCompleted;
  protected Throwable mError;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  /**
   * Reads all bytes of a file.
   */
  @Test
  public void readFullFile() throws Exception {
    long checksumExpected = populateInputFile(CHUNK_SIZE * 10, 0, CHUNK_SIZE * 10 - 1);
    mReadHandler.readBlock(buildReadRequest(0, CHUNK_SIZE * 10), mResponseObserver);
    checkAllReadResponses(mResponseObserver, checksumExpected);
  }

  /**
   * Reads a sub-region of a file.
   */
  @Test
  public void readPartialFile() throws Exception {
    long start = 3;
    long end = CHUNK_SIZE * 10 - 99;
    long checksumExpected = populateInputFile(CHUNK_SIZE * 10, start, end);
    mReadHandler.readBlock(buildReadRequest(start, end + 1 - start), mResponseObserver);
    checkAllReadResponses(mResponseObserver, checksumExpected);
  }

  /**
   * Fails if the read request tries to read an empty file.
   */
  @Test
  public void readEmptyFile() throws Exception {
    populateInputFile(0, 0, 0);
    mExpectedException.expect(InvalidArgumentException.class);
    mReadHandlerNoException.readBlock(buildReadRequest(0, 0), mResponseObserver);
  }

  /**
   * Cancels the read request immediately after the read request is sent.
   */
  @Test
  public void cancelRequest() throws Exception {
    long fileSize = CHUNK_SIZE * 100 + 1;
    populateInputFile(fileSize, 0, fileSize - 1);
    mReadHandler.readBlock(buildReadRequest(0, fileSize), mResponseObserver);
    mReadHandler.onCancel();

    checkCancel(mResponseObserver);
  }

  @Test
  public void ErrorReceived() throws Exception {
    mReadHandler.onError(new IOException("test error"));
  }

  @Test
  public void ErrorReceivedAfterRequest() throws Exception {
    populateInputFile(CHUNK_SIZE * 10, 0, CHUNK_SIZE * 10 - 1);
    mReadHandler.readBlock(buildReadRequest(0, CHUNK_SIZE * 10), mResponseObserver);
    mReadHandler.onError(new IOException("test error"));
  }

  /**
   * Populates the input file, also computes the checksum for part of the file.
   *
   * @param length the length of the file
   * @param start the start position to compute the checksum
   * @param end the last position to compute the checksum
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
  protected void checkAllReadResponses(StreamObserver<ReadResponse> responseObserver,
      long checksumExpected) throws Exception {
    long checksumActual = 0;
    for (ReadResponse readResponse : waitForResponses(responseObserver)) {
      if (readResponse == null) {
        Assert.fail();
        break;
      }
      ByteString buffer = checkReadResponse(readResponse);
      if (buffer != null) {
        for (byte b : buffer) {
          checksumActual += BufferUtils.byteToInt(b);
        }
      }
    }
    Assert.assertEquals(checksumExpected, checksumActual);
  }

  /**
   * Checks the read response message given the expected error code.
   *
   * @param readResponse the read response
   * @return the data buffer extracted from the read response
   */
  protected ByteString checkReadResponse(ReadResponse readResponse) {
    Assert.assertTrue(readResponse.hasChunk());
    Assert.assertTrue(readResponse.getChunk().hasData());
    return readResponse.getChunk().getData();
  }

  protected void checkCancel(StreamObserver<ReadResponse> responseObserver)
      throws TimeoutException, InterruptedException {
    Assert.assertEquals(null, waitForError(responseObserver));
  }

  /**
   * Waits for one read response message.
   *
   * @param responseObserver the response stream observer
   * @return the read response
   */
  protected List<ReadResponse> waitForResponses(final StreamObserver<ReadResponse> responseObserver)
      throws TimeoutException, InterruptedException {
    CommonUtils.waitFor("response", () -> mResponseCompleted || mError != null,
        WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
    ArgumentCaptor<ReadResponse> captor = ArgumentCaptor.forClass(ReadResponse.class);
    verify(responseObserver).onNext(captor.capture());
    return captor.getAllValues();
  }

  /**
   * Waits for one read response message.
   *
   * @param responseObserver the response stream observer
   * @return the read response
   */
  protected Throwable waitForError(final StreamObserver<ReadResponse> responseObserver)
      throws TimeoutException, InterruptedException {
    CommonUtils.waitFor("response", () -> mResponseCompleted || mError != null,
        WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
    ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
    verify(responseObserver).onError(captor.capture());
    return captor.getValue();
  }

  /**
   * Builds a read request.
   *
   * @param offset the offset
   * @param len the length to read
   * @return the proto message
   */
  protected abstract ReadRequest buildReadRequest(long offset, long len);

  /**
   * Mocks the reader (block reader or UFS file reader).
   *
   * @param start the start pos of the reader
   */
  protected abstract void mockReader(long start) throws Exception;
}
