/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.keyvalue;

import alluxio.AlluxioURI;
import alluxio.grpc.CompletePartitionPRequest;
import alluxio.grpc.CompletePartitionPResponse;
import alluxio.grpc.CompleteStorePRequest;
import alluxio.grpc.CompleteStorePResponse;
import alluxio.grpc.CreateStorePRequest;
import alluxio.grpc.CreateStorePResponse;
import alluxio.grpc.DeleteStorePRequest;
import alluxio.grpc.DeleteStorePResponse;
import alluxio.grpc.GetPartitionInfoPRequest;
import alluxio.grpc.GetPartitionInfoPResponse;
import alluxio.grpc.KeyValueMasterClientServiceGrpc;
import alluxio.grpc.MergeStorePRequest;
import alluxio.grpc.MergeStorePResponse;
import alluxio.grpc.RenameStorePRequest;
import alluxio.grpc.RenameStorePResponse;

import alluxio.util.RpcUtilsNew;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a Thrift handler for key-value master RPCs invoked by an Alluxio client.
 */
@ThreadSafe
public final class KeyValueMasterClientServiceHandler
    extends KeyValueMasterClientServiceGrpc.KeyValueMasterClientServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(KeyValueMasterClientServiceHandler.class);

  private final KeyValueMaster mKeyValueMaster;

  /**
   * Constructs the service handler to process incoming RPC calls for key-value master.
   *
   * @param keyValueMaster handler to the real {@link KeyValueMaster} instance
   */
  KeyValueMasterClientServiceHandler(KeyValueMaster keyValueMaster) {
    mKeyValueMaster = keyValueMaster;
  }

  public void completePartition(CompletePartitionPRequest request,
      StreamObserver<CompletePartitionPResponse> responseObserver) {
    RpcUtilsNew.call(LOG,
        (RpcUtilsNew.RpcCallableThrowsIOException<CompletePartitionPResponse>) () -> {
          mKeyValueMaster.completePartition(new AlluxioURI(request.getPath()),
              request.getPartitionInfo());
          return CompletePartitionPResponse.getDefaultInstance();
        }, "completePartition", "request=%s", responseObserver, request);
  }

  /**
   * <pre>
   **
   * Marks a store complete with its filesystem path.
   * </pre>
   */
  public void completeStore(CompleteStorePRequest request,
      StreamObserver<CompleteStorePResponse> responseObserver) {
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<CompleteStorePResponse>) () -> {
      mKeyValueMaster.completeStore(new AlluxioURI(request.getPath()));
      return CompleteStorePResponse.getDefaultInstance();
    }, "completeStore", "request=%s", responseObserver, request);
  }

  /**
   * <pre>
   **
   * Creates a new key-value store on master.
   * </pre>
   */
  public void createStore(CreateStorePRequest request,
      StreamObserver<CreateStorePResponse> responseObserver) {
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<CreateStorePResponse>) () -> {
      mKeyValueMaster.createStore(new AlluxioURI(request.getPath()));
      return CreateStorePResponse.getDefaultInstance();
    }, "createStore", "request=%s", responseObserver, request);
  }

  /**
   * <pre>
   **
   * Deletes a completed key-value store.
   * </pre>
   */
  public void deleteStore(DeleteStorePRequest request,
      StreamObserver<DeleteStorePResponse> responseObserver) {
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<DeleteStorePResponse>) () -> {
      mKeyValueMaster.deleteStore(new AlluxioURI(request.getPath()));
      return DeleteStorePResponse.getDefaultInstance();
    }, "deleteStore", "request=%s", responseObserver, request);
  }

  /**
   * <pre>
   **
   * Gets the partition information for the key-value store at the given filesystem path.
   * </pre>
   */
  public void getPartitionInfo(GetPartitionInfoPRequest request,
      StreamObserver<GetPartitionInfoPResponse> responseObserver) {
    RpcUtilsNew.call(LOG,
        (RpcUtilsNew.RpcCallableThrowsIOException<GetPartitionInfoPResponse>) () -> {
          return GetPartitionInfoPResponse.newBuilder().addAllPartitionInfo(
              mKeyValueMaster.getPartitionInfo(new AlluxioURI(request.getPath()))).build();
        }, "getPartitionInfo", "request=%s", responseObserver, request);
  }

  /**
   * <pre>
   **
   * Merges one completed key-value store to another completed key-value store.
   * </pre>
   */
  public void mergeStore(MergeStorePRequest request,
      StreamObserver<MergeStorePResponse> responseObserver) {
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<MergeStorePResponse>) () -> {
      mKeyValueMaster.mergeStore(new AlluxioURI(request.getFromPath()),
          new AlluxioURI(request.getToPath()));
      return MergeStorePResponse.getDefaultInstance();
    }, "mergeStore", "request=%s", responseObserver, request);
  }

  /**
   * <pre>
   **
   * Renames a completed key-value store.
   * </pre>
   */
  public void renameStore(RenameStorePRequest request,
      StreamObserver<RenameStorePResponse> responseObserver) {
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<RenameStorePResponse>) () -> {
      mKeyValueMaster.renameStore(new AlluxioURI(request.getOldPath()),
          new AlluxioURI(request.getNewPath()));
      return RenameStorePResponse.getDefaultInstance();
    }, "renameStore", "request=%s", responseObserver, request);
  }
}
