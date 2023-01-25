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

import alluxio.exception.status.DeadlineExceededException;
import alluxio.grpc.DataMessage;
import alluxio.grpc.DataMessageMarshaller;
import alluxio.network.protocol.databuffer.DataBuffer;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A helper class for accessing gRPC bi-directional stream synchronously.
 *
 * @param <ReqT> type of the request
 * @param <ResT> type of the response
 */
@NotThreadSafe
public class GrpcDataMessageBlockingStream<ReqT, ResT> extends GrpcBlockingStream<ReqT, ResT> {
  private final DataMessageMarshaller<ReqT> mRequestMarshaller;
  private final DataMessageMarshaller<ResT> mResponseMarshaller;

  /**
   * @param rpcFunc the gRPC bi-directional stream stub function
   * @param bufferSize maximum number of incoming messages the buffer can hold
   * @param description description of this stream
   * @param requestMarshaller the marshaller for the request
   * @param responseMarshaller the marshaller for the response
   */
  public GrpcDataMessageBlockingStream(Function<StreamObserver<ResT>, StreamObserver<ReqT>> rpcFunc,
      int bufferSize, String description, DataMessageMarshaller<ReqT> requestMarshaller,
      DataMessageMarshaller<ResT> responseMarshaller) {
    super((resObserver) -> {
      DataMessageClientResponseObserver<ReqT, ResT> newObserver =
          new DataMessageClientResponseObserver<>(resObserver, requestMarshaller,
              responseMarshaller);
      return rpcFunc.apply(newObserver);
    }, bufferSize, description);
    mRequestMarshaller = requestMarshaller;
    mResponseMarshaller = responseMarshaller;
  }

  @Override
  public ResT receive(long timeoutMs) throws IOException {
    if (mResponseMarshaller == null) {
      return super.receive(timeoutMs);
    }
    DataMessage<ResT, DataBuffer> message = receiveDataMessage(timeoutMs);
    if (message == null) {
      return null;
    }
    return mResponseMarshaller.combineData(message);
  }

  /**
   * Receives a response with data buffer from the server. Will wait until a response is received,
   * or throw an exception if times out. Caller of this method must release the buffer after reading
   * the data.
   *
   * @param timeoutMs maximum time to wait before giving up and throwing
   *                  a {@link DeadlineExceededException}
   * @return the response message with data buffer, or null if the inbound stream is completed
   * @throws IOException if any error occurs
   */
  public DataMessage<ResT, DataBuffer> receiveDataMessage(long timeoutMs) throws IOException {
    Preconditions.checkNotNull(mResponseMarshaller,
        "Cannot retrieve data message without a response marshaller.");
    ResT response = super.receive(timeoutMs);
    if (response == null) {
      return null;
    }
    DataBuffer buffer = mResponseMarshaller.pollBuffer(response);
    return new DataMessage<>(response, buffer);
  }

  /**
   * Sends a request. Will wait until the stream is ready before sending or timeout if the
   * given timeout is reached.
   *
   * @param message the request message with {@link DataBuffer attached}
   * @param timeoutMs maximum wait time before throwing a {@link DeadlineExceededException}
   * @throws IOException if any error occurs
   */
  public void sendDataMessage(DataMessage<ReqT, DataBuffer> message, long timeoutMs)
      throws IOException {
    if (mRequestMarshaller != null) {
      mRequestMarshaller.offerBuffer(message.getBuffer(), message.getMessage());
    }
    super.send(message.getMessage(), timeoutMs);
  }

  @Override
  public Optional<ResT> waitForComplete(long timeoutMs) throws IOException {
    if (mResponseMarshaller == null) {
      return super.waitForComplete(timeoutMs);
    }
    // loop until the last response is received, whose result will be returned
    DataMessage<ResT, DataBuffer> message;
    DataMessage<ResT, DataBuffer> prevMessage = null;
    while (!isCanceled() && (message = receiveDataMessage(timeoutMs)) != null) {
      if (prevMessage != null && prevMessage.getBuffer() != null) {
        prevMessage.getBuffer().release();
      }
      prevMessage = message;
    }
    // note that the combineData call is responsible for releasing the buffer of prevMessage
    ResT result = mResponseMarshaller.combineData(prevMessage);
    return Optional.ofNullable(super.waitForComplete(timeoutMs).orElse(result));
  }
}
