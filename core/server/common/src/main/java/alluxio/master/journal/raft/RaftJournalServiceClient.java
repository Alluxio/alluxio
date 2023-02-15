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
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.DownloadSnapshotPRequest;
import alluxio.grpc.DownloadSnapshotPResponse;
import alluxio.grpc.LatestSnapshotInfoPRequest;
import alluxio.grpc.RaftJournalServiceGrpc;
import alluxio.grpc.ServiceType;
import alluxio.grpc.SnapshotMetadata;
import alluxio.grpc.UploadSnapshotPRequest;
import alluxio.grpc.UploadSnapshotPResponse;
import alluxio.master.MasterClientContext;
import alluxio.master.selectionpolicy.MasterSelectionPolicy;

import io.grpc.stub.StreamObserver;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * A client for raft journal service.
 */
public class RaftJournalServiceClient extends AbstractMasterClient {
  private RaftJournalServiceGrpc.RaftJournalServiceStub mClient = null;

  private final long mRequestInfoTimeoutMs =
      Configuration.getMs(PropertyKey.MASTER_JOURNAL_REQUEST_INFO_TIMEOUT);

  /**
   * Create a client that talks to the leading master.
   */
  public RaftJournalServiceClient() {
    this(MasterSelectionPolicy.Factory.primaryMaster());
  }

  /**
   * Create a client that talks to a specific master.
   * @param selectionPolicy specifies which master is targeted
   */
  public RaftJournalServiceClient(MasterSelectionPolicy selectionPolicy) {
    super(MasterClientContext.newBuilder(ClientContext.create(Configuration.global())).build(),
        selectionPolicy);
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
   * @return {@link SnapshotMetadata} from specified master
   */
  public SnapshotMetadata requestLatestSnapshotInfo() {
    RaftJournalServiceGrpc.RaftJournalServiceBlockingStub client =
        RaftJournalServiceGrpc.newBlockingStub(mChannel);
    return client.withDeadlineAfter(mRequestInfoTimeoutMs, TimeUnit.MILLISECONDS)
        .requestLatestSnapshotInfo(LatestSnapshotInfoPRequest.getDefaultInstance());
  }

  /**
   * Receive snapshot data from specified follower.
   * @param outputDir marks where the data should be outputted
   */
  public void downloadLatestSnapshot(File outputDir) {

//    RaftJournalServiceGrpc.RaftJournalServiceBlockingStub client =
//        RaftJournalServiceGrpc.newBlockingStub(mChannel);
//    client.downloadLatestSnapshot(Empty.newBuilder().build()).forEachRemaining();
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
