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

package alluxio.master.journal.raft;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.grpc.DownloadSnapshotPRequest;
import alluxio.grpc.DownloadSnapshotPResponse;
import alluxio.grpc.RaftJournalServiceGrpc;
import alluxio.grpc.ServiceType;
import alluxio.grpc.UploadSnapshotPRequest;
import alluxio.grpc.UploadSnapshotPResponse;
import alluxio.master.MasterClientContext;

import io.grpc.stub.StreamObserver;

/**
 * A client for raft journal service.
 */
public class RaftJournalServiceClient extends AbstractMasterClient {
  private RaftJournalServiceGrpc.RaftJournalServiceStub mClient = null;

  /**
   * @param clientContext master client context
   */
  public RaftJournalServiceClient(MasterClientContext clientContext) {
    super(clientContext);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.RAFT_JOURNAL_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.RAFT_JOURNAL_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.RAFT_JOURNAL_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = RaftJournalServiceGrpc.newStub(mChannel);
  }

  /**
   * Uploads a snapshot.
   * @param responseObserver the response stream observer
   * @return the request stream observer
   */
  public StreamObserver<UploadSnapshotPRequest> uploadSnapshot(
      StreamObserver<UploadSnapshotPResponse> responseObserver) {
    return mClient.uploadSnapshot(responseObserver);
  }

  /**
   * Downloads a snapshot.
   * @param responseObserver the response stream observer
   * @return the request stream observer
   */
  public StreamObserver<DownloadSnapshotPRequest> downloadSnapshot(
      StreamObserver<DownloadSnapshotPResponse> responseObserver) {
    return mClient.downloadSnapshot(responseObserver);
  }
}
