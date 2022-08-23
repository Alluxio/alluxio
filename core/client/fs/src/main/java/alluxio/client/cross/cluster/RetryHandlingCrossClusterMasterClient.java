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

package alluxio.client.cross.cluster;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.ClusterId;
import alluxio.grpc.CrossClusterMasterClientServiceGrpc;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientContext;
import alluxio.proto.journal.CrossCluster.MountList;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client to cross cluster configuration service.
 */
public class RetryHandlingCrossClusterMasterClient extends AbstractMasterClient
    implements CrossClusterClient {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterClient.class);
  private CrossClusterMasterClientServiceGrpc
      .CrossClusterMasterClientServiceBlockingStub mClient = null;
  private CrossClusterMasterClientServiceGrpc
      .CrossClusterMasterClientServiceStub mClientAsync = null;

  /**
   * @param conf master client configuration
   */
  public RetryHandlingCrossClusterMasterClient(MasterClientContext conf) {
    super(conf);
  }

  @Override
  public void subscribeMounts(String clusterId, StreamObserver<MountList> stream)
      throws AlluxioStatusException {
    retryRPC(() -> {
      mClientAsync.subscribeMounts(ClusterId.newBuilder().setClusterId(clusterId).build(), stream);
      return null;
    }, LOG, "SubscribeMounts", "subscribeMounts=%s", clusterId, stream);
  }

  @Override
  public void setMountList(MountList mountList) throws AlluxioStatusException {
    retryRPC(() -> mClient.setMountList(mountList),
        LOG, "SetMountList", "setMountList=%s", mountList);
  }

  @Override
  protected void afterConnect() {
    mClient = CrossClusterMasterClientServiceGrpc.newBlockingStub(mChannel);
    mClientAsync = CrossClusterMasterClientServiceGrpc.newStub(mChannel);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.CROSS_CLUSTER_MASTER_CLIENT_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.CROSS_CLUSTER_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.CROSS_CLUSTER_MASTER_CLIENT_SERVICE_VERSION;
  }
}
