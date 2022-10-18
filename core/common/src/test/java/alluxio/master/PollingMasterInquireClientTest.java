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

package alluxio.master;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.ConfigurationBuilder;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GetNodeStatePRequest;
import alluxio.grpc.GetNodeStatePResponse;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.JournalMasterClientServiceGrpc;
import alluxio.grpc.NodeState;
import alluxio.network.RejectingServer;
import alluxio.retry.CountingRetry;
import alluxio.util.network.NetworkAddressUtils;

import io.grpc.stub.StreamObserver;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link PollingMasterInquireClient}.
 */
public class PollingMasterInquireClientTest {
  @Rule
  public PortReservationRule mPort = new PortReservationRule();

  @Test(timeout = 10000)
  public void pollRejectingDoesntHang() throws Exception {
    int port = mPort.getPort();
    InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", port);
    RejectingServer s = new RejectingServer(serverAddress);
    s.start();
    List<InetSocketAddress> addrs = Arrays.asList(InetSocketAddress
        .createUnresolved(NetworkAddressUtils.getLocalHostName(Constants.SECOND_MS), port));
    PollingMasterInquireClient client = new PollingMasterInquireClient(addrs,
        () -> new CountingRetry(0), new ConfigurationBuilder().build());
    try {
      client.getPrimaryRpcAddress();
      fail("Expected polling to fail");
    } catch (UnavailableException e) {
      // Expected
    }
  }

  @Test(timeout = 10000)
  public void getPrimaryRpcAddress() throws IOException {
    int port = mPort.getPort();
    InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", port);

    JournalMasterClientServiceGrpc.JournalMasterClientServiceImplBase handler =
        new JournalMasterClientServiceGrpc.JournalMasterClientServiceImplBase() {
          @Override
          public void getNodeState(GetNodeStatePRequest request,
              StreamObserver<GetNodeStatePResponse> responseObserver) {
            responseObserver.onNext(
                GetNodeStatePResponse.newBuilder().setNodeState(NodeState.PRIMARY).build());
            responseObserver.onCompleted();
          }
        };

    GrpcServer grpcServer = GrpcServerBuilder
        .forAddress(
            GrpcServerAddress.create("localhost", serverAddress), Configuration.global())
        .addService(new GrpcService(handler))
        .build();

    try {
      grpcServer.start();
      List<InetSocketAddress> addrs = Arrays.asList(serverAddress);
      PollingMasterInquireClient client = new PollingMasterInquireClient(addrs,
          () -> new CountingRetry(0), new ConfigurationBuilder().build());
      assertEquals(serverAddress, client.getPrimaryRpcAddress());
    } catch (Exception e) {
      throw e;
    } finally {
      grpcServer.shutdown();
    }
  }
}
