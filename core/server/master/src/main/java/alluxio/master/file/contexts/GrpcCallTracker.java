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

package alluxio.master.file.contexts;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * {@link CallTracker} implementation for gRPC calls.
 */
public class GrpcCallTracker implements CallTracker {

  /** Server-side gRPC stream observer. */
  private ServerCallStreamObserver<?> mStreamObserver;

  /**
   * Creates a call tracker for gRPC.
   *
   * @param streamObserver server stream observer
   */
  public GrpcCallTracker(StreamObserver<?> streamObserver) {
    if (!(streamObserver instanceof ServerCallStreamObserver)) {
      throw new IllegalStateException(String.format(
          "Call tracking is only supported for server streams. Passed stream type: %s",
          streamObserver.getClass().getSimpleName()));
    }
    mStreamObserver = (ServerCallStreamObserver) streamObserver;
  }

  @Override
  public boolean isCancelled() {
    return mStreamObserver.isCancelled();
  }

  @Override
  public Type getType() {
    return Type.GRPC_CLIENT_TRACKER;
  }
}
