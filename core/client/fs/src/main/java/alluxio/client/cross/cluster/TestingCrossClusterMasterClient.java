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

import alluxio.grpc.ClusterId;
import alluxio.grpc.CrossClusterMasterClientServiceGrpc;
import alluxio.proto.journal.CrossCluster.MountList;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * A client for testing cross cluster configuration.
 */
public class TestingCrossClusterMasterClient implements CrossClusterClient {

  public final CrossClusterMasterClientServiceGrpc
      .CrossClusterMasterClientServiceBlockingStub mClient;
  public final CrossClusterMasterClientServiceGrpc
      .CrossClusterMasterClientServiceStub mClientAsync;

  /**
   * @param channel the channel
   */
  public TestingCrossClusterMasterClient(Channel channel) {
    mClient = CrossClusterMasterClientServiceGrpc.newBlockingStub(channel);
    mClientAsync = CrossClusterMasterClientServiceGrpc.newStub(channel);
  }

  @Override
  public void subscribeMounts(String clusterId, StreamObserver<MountList> stream) {
    mClientAsync.subscribeMounts(ClusterId.newBuilder().setClusterId(clusterId).build(), stream);
  }

  @Override
  public void setMountList(MountList mountList) {
    mClient.setMountList(mountList);
  }

  @Override
  public void connect() throws IOException {
  }

  @Override
  public void disconnect() {
  }

  @Override
  public SocketAddress getRemoteSockAddress() {
    return null;
  }

  @Override
  public String getRemoteHostName() {
    return null;
  }

  @Override
  public InetSocketAddress getConfAddress() {
    return null;
  }

  @Override
  public boolean isConnected() {
    return false;
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void close() throws IOException {
  }
}
