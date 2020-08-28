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

import alluxio.grpc.DataMessageMarshaller;
import alluxio.grpc.DataMessageMarshallerProvider;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link StreamObserver} that handles raw data buffers.
 *
 * @param <RespT> type of the response message
 * @param <ReqT> type of the request message
 */
@NotThreadSafe
public class DataMessageClientResponseObserver<ReqT, RespT>
    extends DataMessageMarshallerProvider<ReqT, RespT>
    implements ClientResponseObserver<ReqT, RespT> {
  private static final Logger LOG =
      LoggerFactory.getLogger(DataMessageClientResponseObserver.class);

  private final StreamObserver<RespT> mObserver;

  /**
   * @param observer the original response observer
   * @param requestMarshaller the marshaller for the request
   * @param responseMarshaller the marshaller for the response
   */
  public DataMessageClientResponseObserver(StreamObserver<RespT> observer,
      DataMessageMarshaller<ReqT> requestMarshaller,
      DataMessageMarshaller<RespT> responseMarshaller) {
    super(requestMarshaller, responseMarshaller);
    mObserver = observer;
  }

  @Override
  public void onNext(RespT value) {
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
  public void beforeStart(ClientCallStreamObserver<ReqT> requestStream) {
    if (mObserver instanceof ClientResponseObserver) {
      ((ClientResponseObserver<ReqT, RespT>) mObserver).beforeStart(requestStream);
    } else {
      LOG.warn("{} does not implement ClientResponseObserver:beforeStart", mObserver);
    }
  }
}
