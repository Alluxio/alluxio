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

import static org.junit.Assert.assertThrows;

import alluxio.Constants;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.ConfigurationBuilder;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.grpc.ServiceVersionClientServiceGrpc;
import alluxio.network.RejectingServer;
import alluxio.retry.CountingRetry;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.Rule;
import org.junit.Test;

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
        () -> new CountingRetry(0), new ConfigurationBuilder().build(),
        ServiceType.META_MASTER_CLIENT_SERVICE);
    assertThrows("Expected polling to fail", UnavailableException.class,
        client::getPrimaryRpcAddress);
  }

  @Test(timeout = 10000)
  public void concurrentPollingMaster() throws Exception {
    int port1 = PortRegistry.reservePort();
    int port2 = PortRegistry.reservePort();
    InetSocketAddress serverAddress1 = new InetSocketAddress("127.0.0.1", port1);
    InetSocketAddress serverAddress2 = new InetSocketAddress("127.0.0.1", port2);
    RejectingServer s1 = new RejectingServer(serverAddress1, 20000);
    GrpcServer s2 =
        GrpcServerBuilder.forAddress(GrpcServerAddress.create(serverAddress2),
                new InstancedConfiguration(new AlluxioProperties()))
            .addService(ServiceType.META_MASTER_CLIENT_SERVICE, new GrpcService(
                new ServiceVersionClientServiceGrpc.ServiceVersionClientServiceImplBase() {
                })).build();
    try {
      s1.start();
      s2.start();
      List<InetSocketAddress> addrs =
          Arrays.asList(InetSocketAddress.createUnresolved("127.0.0.1", port1),
              InetSocketAddress.createUnresolved("127.0.0.1", port2));
      PollingMasterInquireClient client = new PollingMasterInquireClient(addrs,
          () -> new CountingRetry(0),
          new ConfigurationBuilder()
              .setProperty(PropertyKey.USER_MASTER_POLLING_CONCURRENT, true)
              .build(),
          ServiceType.META_MASTER_CLIENT_SERVICE);
      client.getPrimaryRpcAddress();
    } finally {
      s1.stopAndJoin();
      s2.shutdown();
      PortRegistry.release(port1);
      PortRegistry.release(port2);
    }
  }
}
