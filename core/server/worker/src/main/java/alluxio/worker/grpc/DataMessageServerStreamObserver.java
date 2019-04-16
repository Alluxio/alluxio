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

import alluxio.grpc.BufferRepository;
import alluxio.grpc.DataMessage;
import alluxio.network.protocol.databuffer.DataBuffer;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link StreamObserver} for handling raw data buffers.
 *
 * @param <T> type of the message
 */
@NotThreadSafe
public class DataMessageServerStreamObserver<T> extends CallStreamObserver<T> {

  private final BufferRepository<T, DataBuffer> mBufferRepository;
  private final CallStreamObserver<T> mObserver;

  /**
   * @param observer the original observer
   * @param bufferRepository the repository of the buffers
   */
  public DataMessageServerStreamObserver(CallStreamObserver<T> observer,
      BufferRepository<T, DataBuffer> bufferRepository) {
    mObserver = observer;
    mBufferRepository = bufferRepository;
  }

  /**
   * Receives a message with data buffer from the stream.
   *
   * @param value the value passed to the stream
   */
  public void onNext(DataMessage<T, DataBuffer> value) {
    DataBuffer buffer = value.getBuffer();
    if (buffer != null) {
      mBufferRepository.offerBuffer(buffer, value.getMessage());
    }
    mObserver.onNext(value.getMessage());
  }

  @Override
  public void onNext(T value) {
    mObserver.onNext(value);
  }

  @Override
  public void onError(Throwable t) {
    mObserver.onError(t);
  }

  @Override
  public void onCompleted() {
    mObserver.onCompleted();
  }

  @Override
  public boolean isReady() {
    return mObserver.isReady();
  }

  @Override
  public void setOnReadyHandler(Runnable onReadyHandler) {
    mObserver.setOnReadyHandler(onReadyHandler);
  }

  @Override
  public void disableAutoInboundFlowControl() {
    mObserver.disableAutoInboundFlowControl();
  }

  @Override
  public void request(int count) {
    mObserver.request(count);
  }

  @Override
  public void setMessageCompression(boolean enable) {
    mObserver.setMessageCompression(enable);
  }
}
