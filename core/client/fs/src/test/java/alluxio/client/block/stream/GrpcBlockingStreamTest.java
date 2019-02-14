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

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.exception.status.CanceledException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.util.ThreadFactoryUtils;

import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Set up gRPC interface mocks.
   */
  @Before
  public void before() {
    mClient = mock(BlockWorkerClient.class);
    mRequestObserver = mock(ClientCallStreamObserver.class);
    when(mClient.writeBlock(any(StreamObserver.class))).thenAnswer((args) -> {
      mResponseObserver = args.getArgumentAt(0, ClientResponseObserver.class);
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

    Assert.assertEquals(response, actualResponse);
  }

  /**
   * Checks onCompleted is called on request observer upon close.
   */
  @Test
  public void close() throws Exception {
    mStream.close();

    Assert.assertTrue(mStream.isClosed());
    Assert.assertFalse(mStream.isOpen());
    verify(mRequestObserver).onCompleted();
  }

  /**
   * Checks cancel is called on request observer upon cancel.
   */
  @Test
  public void cancel() throws Exception {
    mStream.cancel();

    Assert.assertTrue(mStream.isCanceled());
    Assert.assertFalse(mStream.isOpen());
    verify(mRequestObserver).cancel(any(String.class), eq(null));
  }

  /**
   * Checks onCompleted posted on the response observer is received.
   */
  @Test
  public void onCompleted() throws Exception {
    mResponseObserver.onCompleted();

    WriteResponse actualResponse = mStream.receive(TIMEOUT);
    Assert.assertNull(actualResponse);
  }

  /**
   * Checks expected error during send.
   */
  @Test
  public void sendError() throws Exception {
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage(containsString(TEST_MESSAGE));
    mResponseObserver.onError(Status.UNAUTHENTICATED.asRuntimeException());

    mStream.send(WriteRequest.newBuilder().build(), TIMEOUT);
  }

  /**
   * Checks send fails after stream is closed.
   */
  @Test
  public void sendFailsAfterClosed() throws Exception {
    mStream.close();
    mThrown.expect(CanceledException.class);
    mThrown.expectMessage(containsString(TEST_MESSAGE));

    mStream.send(WriteRequest.newBuilder().build(), TIMEOUT);
  }

  /**
   * Checks send fails after stream is canceled.
   */
  @Test
  public void sendFailsAfterCanceled() throws Exception {
    mStream.cancel();
    mThrown.expect(CanceledException.class);
    mThrown.expectMessage(containsString(TEST_MESSAGE));

    mStream.send(WriteRequest.newBuilder().build(), TIMEOUT);
  }

  /**
   * Checks receive fails after stream is canceled.
   */
  @Test
  public void receiveFailsAfterCanceled() throws Exception {
    mStream.cancel();
    mThrown.expect(CanceledException.class);
    mThrown.expectMessage(containsString(TEST_MESSAGE));

    mStream.receive(TIMEOUT);
  }

  /**
   * Checks expected error during receive.
   */
  @Test
  public void receiveError() throws Exception {
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage(containsString(TEST_MESSAGE));
    mResponseObserver.onError(Status.UNAUTHENTICATED.asRuntimeException());

    mStream.receive(TIMEOUT);
  }

  /**
   * Checks send fails after timeout waiting for stream to be ready.
   */
  @Test
  public void sendFailsAfterTimeout() throws Exception {
    when(mRequestObserver.isReady()).thenReturn(false);
    mThrown.expect(DeadlineExceededException.class);
    mThrown.expectMessage(containsString(TEST_MESSAGE));

    mStream.send(WriteRequest.newBuilder().build(), SHORT_TIMEOUT);
  }

  /**
   * Checks receive fails after timeout waiting for message from response stream.
   */
  @Test
  public void receiveFailsAfterTimeout() throws Exception {
    mThrown.expect(DeadlineExceededException.class);
    mThrown.expectMessage(containsString(TEST_MESSAGE));

    mStream.receive(SHORT_TIMEOUT);
  }

  /**
   * Checks send after stream is ready.
   */
  @Test
  public void sendAfterStreamReady() throws Exception {
    when(mRequestObserver.isReady()).thenReturn(false);
    doAnswer((args) -> {
      mOnReadyHandler = args.getArgumentAt(0, Runnable.class);
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

    Assert.assertEquals(response, actualResponse);
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
      Assert.assertEquals(response, actualResponse);
    }
  }

  /**
   * Checks receive fails immediately upon error even if buffer is full.
   */
  @Test
  public void receiveErrorWhenBufferFull() throws Exception {
    mThrown.expect(UnauthenticatedException.class);
    mThrown.expectMessage(containsString(TEST_MESSAGE));
    WriteResponse[] responses = Stream.generate(() -> WriteResponse.newBuilder().build())
        .limit(BUFFER_SIZE).toArray(WriteResponse[]::new);
    for (WriteResponse response : responses) {
      mResponseObserver.onNext(response);
    }
    mResponseObserver.onError(Status.UNAUTHENTICATED.asRuntimeException());

    for (WriteResponse response : responses) {
      WriteResponse actualResponse = mStream.receive(TIMEOUT);
      Assert.assertEquals(response, actualResponse);
    }
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
    Assert.assertEquals(responses[0], actualResponse);

    mStream.waitForComplete(TIMEOUT);

    actualResponse = mStream.receive(TIMEOUT);
    Assert.assertEquals(null, actualResponse);
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
    Assert.assertEquals(responses[0], actualResponse);
    mThrown.expect(DeadlineExceededException.class);
    mThrown.expectMessage(containsString(TEST_MESSAGE));

    mStream.waitForComplete(SHORT_TIMEOUT);
  }
}
