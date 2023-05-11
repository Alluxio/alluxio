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

import alluxio.grpc.DataMessageMarshaller;
import alluxio.grpc.DataMessageMarshallerProvider;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link StreamObserver} that handles raw data buffers.
 *
 * @param <ResT> type of the response message
 * @param <ReqT> type of the request message
 */
@NotThreadSafe
public class DataMessageServerRequestObserver<ReqT, ResT>
    extends DataMessageMarshallerProvider<ReqT, ResT> implements StreamObserver<ResT> {
  private static final Logger LOG =
      LoggerFactory.getLogger(DataMessageServerRequestObserver.class);

  private final StreamObserver<ResT> mObserver;

  /**
   * @param observer the original response observer
   * @param requestMarshaller the marshaller for the request
   * @param responseMarshaller the marshaller for the response
   */
  public DataMessageServerRequestObserver(StreamObserver<ResT> observer,
      DataMessageMarshaller<ReqT> requestMarshaller,
      DataMessageMarshaller<ResT> responseMarshaller) {
    super(requestMarshaller, responseMarshaller);
    mObserver = observer;
  }

  @Override
  public void onNext(ResT value) {
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
}
