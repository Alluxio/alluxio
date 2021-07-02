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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockReadRequest;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.NoopBlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.util.ResourceLeakDetector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * Unit tests for {@link BlockReadHandler}.
 */
public class BlockReadHandlerTest {
  private static final long CHUNK_SIZE =
      ServerConfiguration.getBytes(PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES);
  private final Random mRandom = new Random();

  private BlockReadHandler mReadHandler;
  private ServerCallStreamObserver<ReadResponse> mResponseObserver;
  private List<ReadResponse> mResponses = new ArrayList<>();
  private boolean mResponseCompleted;
  private Throwable mError;
  private BlockWorker mBlockWorker;
  private BlockReader mBlockReader;
  private File mFile;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  @Before
  public void before() throws Exception {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);

    mFile = mTestFolder.newFile();
    mBlockReader = new LocalFileBlockReader(mFile.getPath());
    mBlockWorker = new NoopBlockWorker() {
      @Override
      public BlockReader createBlockReader(BlockReadRequest request) throws IOException {
        ((FileChannel) mBlockReader.getChannel()).position(request.getStart());
        return mBlockReader;
      }
    };
    mResponseObserver = Mockito.mock(ServerCallStreamObserver.class);
    Mockito.when(mResponseObserver.isReady()).thenReturn(true);
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
      mResponses.add(ReadResponse.parseFrom(
          args.getArgument(0, ReadResponse.class).toByteString()));
      return null;
    }).when(mResponseObserver).onNext(any(ReadResponse.class));
    mReadHandler = new BlockReadHandler(GrpcExecutors.BLOCK_READER_EXECUTOR, mBlockWorker,
        mResponseObserver, new AuthenticatedUserInfo(), false);
  }

  /**
   * Reads all bytes of a file.
   */
  @Test
  public void readFullFile() throws Exception {
    long checksumExpected = populateInputFile(CHUNK_SIZE * 10, 0, CHUNK_SIZE * 10 - 1);
    mReadHandler.onNext(buildReadRequest(0, CHUNK_SIZE * 10));
    checkAllReadResponses(mResponses, checksumExpected);
  }

  /**
   * Reads a sub-region of a file.
   */
  @Test
  public void readPartialFile() throws Exception {
    long start = 3;
    long end = CHUNK_SIZE * 10 - 99;
    long checksumExpected = populateInputFile(CHUNK_SIZE * 10, start, end);
    mReadHandler.onNext(buildReadRequest(start, end + 1 - start));
    checkAllReadResponses(mResponses, checksumExpected);
  }

  /**
   * Fails if the read request tries to read an empty file.
   */
  @Test
  public void readEmptyFile() throws Exception {
    populateInputFile(0, 0, 0);
    mReadHandler.onNext(buildReadRequest(0, 0));
    checkErrorCode(mResponseObserver, Status.Code.INVALID_ARGUMENT);
  }

  /**
   * Cancels the read request immediately after the read request is sent.
   */
  @Test
  public void cancelRequest() throws Exception {
    long fileSize = CHUNK_SIZE * 100 + 1;
    populateInputFile(fileSize, 0, fileSize - 1);
    mReadHandler.onNext(buildReadRequest(0, fileSize));
    mReadHandler.onCompleted();

    CommonUtils.waitFor("response", () -> mResponseCompleted || mError != null,
        WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
    verify((StreamObserver<ReadResponse>) mResponseObserver).onCompleted();
  }

  @Test
  public void ErrorReceived() throws Exception {
    mReadHandler.onError(new IOException("test error"));
  }

  @Test
  public void ErrorReceivedAfterRequest() throws Exception {
    populateInputFile(CHUNK_SIZE * 10, 0, CHUNK_SIZE * 10 - 1);
    mReadHandler.onNext(buildReadRequest(0, CHUNK_SIZE * 10));
    mReadHandler.onError(new IOException("test error"));
  }

  @Test
  public void readFailure() throws Exception {
    long fileSize = CHUNK_SIZE * 10 + 1;
    populateInputFile(0, 0, fileSize - 1);
    mBlockReader.close();
    mReadHandler.onNext(buildReadRequest(0, fileSize));
    checkErrorCode(mResponseObserver, Status.Code.FAILED_PRECONDITION);
  }

  /**
   * Populates the input file, also computes the checksum for part of the file.
   *
   * @param length the length of the file
   * @param start the start position to compute the checksum
   * @param end the last position to compute the checksum
   * @return the checksum
   */
  private long populateInputFile(long length, long start, long end) throws Exception {
    long checksum = 0;
    long pos = 0;
    if (length > 0) {
      try (FileOutputStream fileOutputStream = new FileOutputStream(mFile)) {
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
      }
    }
    return checksum;
  }

  /**
   * Checks all the read responses.
   */
  private void checkAllReadResponses(List<ReadResponse> responses,
      long checksumExpected) throws Exception {
    long checksumActual = 0;
    CommonUtils.waitFor("response", () -> mResponseCompleted || mError != null,
        WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
    for (ReadResponse readResponse : responses) {
      if (readResponse == null) {
        Assert.fail();
        break;
      }
      assertTrue(readResponse.hasChunk());
      assertTrue(readResponse.getChunk().hasData());
      ByteString buffer = readResponse.getChunk().getData();
      if (buffer != null) {
        for (byte b : buffer) {
          checksumActual += BufferUtils.byteToInt(b);
        }
      }
    }
    assertEquals(checksumExpected, checksumActual);
  }

  /**
   * Checks an error is returned with given code.
   *
   * @param responseObserver the response stream observer
   * @param code the expected error code
   */
  private void checkErrorCode(final StreamObserver<ReadResponse> responseObserver,
      Status.Code code) throws TimeoutException, InterruptedException {
    CommonUtils.waitFor("response", () -> mResponseCompleted || mError != null,
        WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
    ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
    verify(responseObserver).onError(captor.capture());
    Throwable t = captor.getValue();
    assertTrue(t instanceof StatusException);
    assertEquals(code, ((StatusException) t).getStatus().getCode());
  }

  private ReadRequest buildReadRequest(long offset, long len) {
    ReadRequest readRequest =
        ReadRequest.newBuilder().setBlockId(1L).setOffset(offset).setLength(len)
            .setChunkSize(CHUNK_SIZE).build();
    return readRequest;
  }
}
