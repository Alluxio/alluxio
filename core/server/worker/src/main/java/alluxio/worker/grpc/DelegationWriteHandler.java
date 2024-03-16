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
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.DefaultBlockWorker;

import io.grpc.stub.StreamObserver;

/**
 * A write request handler that delegate worker write request of different types
 * to corresponding write handlers.
 */
public class DelegationWriteHandler implements StreamObserver<alluxio.grpc.WriteRequest> {
  private final StreamObserver<WriteResponse> mResponseObserver;
  private final DefaultBlockWorker mBlockWorker;
  private final UfsManager mUfsManager;

  // if zero-copy is enabled, we will get a custom marshaller to perform
  // the deserialization
  private final DataMessageMarshaller<WriteRequest> mMarshaller;
  private AbstractWriteHandler mWriteHandler;
  private final AuthenticatedUserInfo mUserInfo;
  private final boolean mDomainSocketEnabled;

  /**
   * @param blockWorker the block worker instance
   * @param ufsManager the UFS manager
   * @param responseObserver the response observer of the gRPC stream
   * @param userInfo the authenticated user info
   * @param domainSocketEnabled whether using a domain socket
   */
  public DelegationWriteHandler(DefaultBlockWorker blockWorker, UfsManager ufsManager,
      StreamObserver<WriteResponse> responseObserver, AuthenticatedUserInfo userInfo,
      boolean domainSocketEnabled) {
    mBlockWorker = blockWorker;
    mUfsManager = ufsManager;
    mResponseObserver = responseObserver;
    mUserInfo = userInfo;
    if (mResponseObserver instanceof DataMessageMarshallerProvider) {
      // if zero-copy is enabled, we should get a DataMessageMarshallerProvider,
      // get the custom marshaller
      mMarshaller = ((DataMessageMarshallerProvider<WriteRequest, WriteResponse>) mResponseObserver)
          .getRequestMarshaller().orElse(null);
    } else {
      mMarshaller = null;
    }
    mDomainSocketEnabled = domainSocketEnabled;
  }

  private AbstractWriteHandler createWriterHandler(alluxio.grpc.WriteRequest request) {
    switch (request.getCommand().getType()) {
      case ALLUXIO_BLOCK:
        return new BlockWriteHandler(mBlockWorker, mResponseObserver,
            mUserInfo, mDomainSocketEnabled);
      case UFS_FILE:
        return new UfsFileWriteHandler(mUfsManager, mResponseObserver,
            mUserInfo);
      case UFS_FALLBACK_BLOCK:
        return new UfsFallbackBlockWriteHandler(
            mBlockWorker, mUfsManager, mResponseObserver, mUserInfo, mDomainSocketEnabled);
      default:
        throw new IllegalArgumentException(String.format("Invalid request type %s",
            request.getCommand().getType().name()));
    }
  }

  @Override
  public void onNext(WriteRequest request) {
    if (mWriteHandler == null) {
      mWriteHandler = createWriterHandler(request);
    }
    if (mMarshaller != null) {
      mWriteHandler.writeDataMessage(request, mMarshaller.pollBuffer(request));
    } else {
      mWriteHandler.write(request);
    }
  }

  @Override
  public void onError(Throwable t) {
    if (mWriteHandler != null) {
      mWriteHandler.onError(t);
    }
  }

  @Override
  public void onCompleted() {
    if (mWriteHandler != null) {
      mWriteHandler.onCompleted();
    }
  }

  /**
   * Handles cancel event from the client.
   */
  public void onCancel() {
    if (mWriteHandler != null) {
      mWriteHandler.onCancel();
    }
  }
}
