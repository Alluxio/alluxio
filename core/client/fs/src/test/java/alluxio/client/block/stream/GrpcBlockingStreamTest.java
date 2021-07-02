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

package alluxio.client.block.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.exception.status.CancelledException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.util.ThreadFactoryUtils;

import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * Tests for {@link GrpcBlockingStream}.
 */
public final class GrpcBlockingStreamTest {
  private static final int BUFFER_SIZE = 5;
  private static final long TIMEOUT = 10 * Constants.SECOND_MS;
  private static final long SHORT_TIMEOUT = Constants.SECOND / 2;
  private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4,
      ThreadFactoryUtils.build("test-executor-%d", true));

  private static final String TEST_MESSAGE = "test message";

  private BlockWorkerClient mClient;
  private ClientCallStreamObserver<WriteRequest> mRequestObserver;
  private ClientResponseObserver<WriteRequest, WriteResponse> mResponseObserver;
  private GrpcBlockingStream<WriteRequest, WriteResponse> mStream;
  private Runnable mOnReadyHandler;

  /**
   * Set up gRPC interface mocks.
   */
  @Before
  public void before() {
    mClient = mock(BlockWorkerClient.class);
    mRequestObserver = mock(ClientCallStreamObserver.class);
    when(mClient.writeBlock(any(StreamObserver.class))).thenAnswer((args) -> {
      mResponseObserver = args.getArgument(0, ClientResponseObserver.class);
      return mRequestObserver;
    });
    when(mRequestObserver.isReady()).thenReturn(true);
    mStream = new GrpcBlockingStream<>(mClient::writeBlock, BUFFER_SIZE, TEST_MESSAGE);
  }

  /**
   * Checks send request is called on the request observer.
   */
  @Test
  public void send() throws Exception {
    WriteRequest request = WriteRequest.newBuilder().build();

    mStream.send(request, TIMEOUT);

    verify(mRequestObserver).onNext(request);
  }

  /**
   * Checks response posted on the response observer is received.
   */
  @Test
  public void receive() throws Exception {
    WriteResponse response = WriteResponse.newBuilder().build();
    mResponseObserver.onNext(response);

    WriteResponse actualResponse = mStream.receive(TIMEOUT);

    assertEquals(response, actualResponse);
  }

  /**
   * Checks onCompleted is called on request observer upon close.
   */
  @Test
  public void close() throws Exception {
    mStream.close();

    assertTrue(mStream.isClosed());
    assertFalse(mStream.isOpen());
    verify(mRequestObserver).onCompleted();
  }

  /**
   * Checks cancel is called on request observer upon cancel.
   */
  @Test
  public void cancel() throws Exception {
    mStream.cancel();

    assertTrue(mStream.isCanceled());
    assertFalse(mStream.isOpen());
    verify(mRequestObserver).cancel(any(String.class), eq(null));
  }

  /**
   * Checks onCompleted posted on the response observer is received.
   */
  @Test
  public void onCompleted() throws Exception {
    mResponseObserver.onCompleted();

    WriteResponse actualResponse = mStream.receive(TIMEOUT);
    assertNull(actualResponse);
  }

  /**
   * Checks expected error during send.
   */
  @Test
  public void sendError() throws Exception {
    mResponseObserver.onError(Status.UNAUTHENTICATED.asRuntimeException());
    Exception e = assertThrows(UnauthenticatedException.class,
        () -> mStream.send(WriteRequest.newBuilder().build(), TIMEOUT));
    assertTrue(e.getMessage().contains(TEST_MESSAGE));
  }

  /**
   * Checks send fails after stream is closed.
   */
  @Test
  public void sendFailsAfterClosed() throws Exception {
    mStream.close();
    Exception e = assertThrows(CancelledException.class,
        () -> mStream.send(WriteRequest.newBuilder().build(), TIMEOUT));
    assertTrue(e.getMessage().contains(TEST_MESSAGE));
  }

  /**
   * Checks send fails after stream is canceled.
   */
  @Test
  public void sendFailsAfterCanceled() throws Exception {
    mStream.cancel();
    Exception e = assertThrows(CancelledException.class, () ->
            mStream.send(WriteRequest.newBuilder().build(), TIMEOUT));
    assertTrue(e.getMessage().contains(TEST_MESSAGE));
  }

  /**
   * Checks receive fails after stream is canceled.
   */
  @Test
  public void receiveFailsAfterCanceled() throws Exception {
    mStream.cancel();
    Exception e = assertThrows(CancelledException.class, () ->
        mStream.receive(TIMEOUT));
    assertTrue(e.getMessage().contains(TEST_MESSAGE));
  }

  /**
   * Checks expected error during receive.
   */
  @Test
  public void receiveError() throws Exception {
    mResponseObserver.onError(Status.UNAUTHENTICATED.asRuntimeException());
    Exception e = assertThrows(UnauthenticatedException.class, () ->
        mStream.receive(TIMEOUT));
    assertTrue(e.getMessage().contains(TEST_MESSAGE));
  }

  /**
   * Checks send fails after timeout waiting for stream to be ready.
   */
  @Test
  public void sendFailsAfterTimeout() throws Exception {
    when(mRequestObserver.isReady()).thenReturn(false);
    Exception e = assertThrows(DeadlineExceededException.class, () ->
        mStream.send(WriteRequest.newBuilder().build(), SHORT_TIMEOUT));
    assertTrue(e.getMessage().contains(TEST_MESSAGE));
  }

  /**
   * Checks receive fails after timeout waiting for message from response stream.
   */
  @Test
  public void receiveFailsAfterTimeout() throws Exception {
    Exception e = assertThrows(DeadlineExceededException.class, () ->
        mStream.receive(SHORT_TIMEOUT));
    assertTrue(e.getMessage().contains(TEST_MESSAGE));
  }

  /**
   * Checks send after stream is ready.
   */
  @Test
  public void sendAfterStreamReady() throws Exception {
    when(mRequestObserver.isReady()).thenReturn(false);
    doAnswer((args) -> {
      mOnReadyHandler = args.getArgument(0, Runnable.class);
      return null;
    }).when(mRequestObserver).setOnReadyHandler(any(Runnable.class));
    mResponseObserver.beforeStart(mRequestObserver);
    EXECUTOR.submit(() -> {
      try {
        // notify ready after a short period of time
        Thread.sleep(SHORT_TIMEOUT);
        when(mRequestObserver.isReady()).thenReturn(true);
        mOnReadyHandler.run();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    WriteRequest request = WriteRequest.newBuilder().build();

    mStream.send(request, TIMEOUT);

    verify(mRequestObserver).onNext(request);
  }

  /**
   * Checks receive after response arrives.
   */
  @Test
  public void receiveAfterResponseArrives() throws Exception {
    WriteResponse response = WriteResponse.newBuilder().build();
    EXECUTOR.submit(() -> {
      try {
        // push response after a short period of time
        Thread.sleep(SHORT_TIMEOUT);
        mResponseObserver.onNext(response);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    WriteResponse actualResponse = mStream.receive(TIMEOUT);

    assertEquals(response, actualResponse);
  }

  /**
   * Checks receive responses more than buffer size in order.
   */
  @Test
  public void receiveMoreThanBufferSize() throws Exception {
    WriteResponse[] responses = Stream.generate(() -> WriteResponse.newBuilder().build())
        .limit(BUFFER_SIZE * 2).toArray(WriteResponse[]::new);

    EXECUTOR.submit(() -> {
      for (WriteResponse response : responses) {
        mResponseObserver.onNext(response);
      }
    });
    Thread.sleep(SHORT_TIMEOUT);

    for (WriteResponse response : responses) {
      WriteResponse actualResponse = mStream.receive(TIMEOUT);
      assertEquals(response, actualResponse);
    }
  }

  /**
   * Checks receive fails immediately upon error even if buffer is full.
   */
  @Test
  public void receiveErrorWhenBufferFull() throws Exception {
    WriteResponse[] responses = Stream.generate(() -> WriteResponse.newBuilder().build())
        .limit(BUFFER_SIZE).toArray(WriteResponse[]::new);
    for (WriteResponse response : responses) {
      mResponseObserver.onNext(response);
    }
    mResponseObserver.onError(Status.UNAUTHENTICATED.asRuntimeException());

    Exception e = assertThrows(UnauthenticatedException.class, () -> {
      for (WriteResponse response : responses) {
        WriteResponse actualResponse = mStream.receive(TIMEOUT);
        assertEquals(response, actualResponse);
      }
    });
    assertTrue(e.getMessage().contains(TEST_MESSAGE));
  }

  /**
   * Checks waitForComplete succeed after onCompleted is triggered on response stream.
   */
  @Test
  public void waitForComplete() throws Exception {
    WriteResponse[] responses = Stream.generate(() -> WriteResponse.newBuilder().build())
        .limit(BUFFER_SIZE * 2).toArray(WriteResponse[]::new);

    EXECUTOR.submit(() -> {
      for (WriteResponse response : responses) {
        mResponseObserver.onNext(response);
      }
      try {
        Thread.sleep(SHORT_TIMEOUT);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      mResponseObserver.onCompleted();
    });

    WriteResponse actualResponse = mStream.receive(TIMEOUT);
    assertEquals(responses[0], actualResponse);

    mStream.waitForComplete(TIMEOUT);

    actualResponse = mStream.receive(TIMEOUT);
    assertEquals(null, actualResponse);
  }

  /**
   * Checks waitForComplete fails after times out.
   */
  @Test
  public void waitForCompleteTimeout() throws Exception {
    WriteResponse[] responses = Stream.generate(() -> WriteResponse.newBuilder().build())
        .limit(BUFFER_SIZE * 2).toArray(WriteResponse[]::new);

    EXECUTOR.submit(() -> {
      for (WriteResponse response : responses) {
        mResponseObserver.onNext(response);
      }
      try {
        Thread.sleep(TIMEOUT);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      mResponseObserver.onCompleted();
    });

    WriteResponse actualResponse = mStream.receive(SHORT_TIMEOUT);
    assertEquals(responses[0], actualResponse);

    Exception e = assertThrows(DeadlineExceededException.class, () ->
        mStream.waitForComplete(SHORT_TIMEOUT));
    assertTrue(e.getMessage().contains(TEST_MESSAGE));
  }
}
