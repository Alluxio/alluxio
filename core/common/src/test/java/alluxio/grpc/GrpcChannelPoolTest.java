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

package alluxio.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link GrpcChannelPool}.
 */
public final class GrpcChannelPoolTest {

  private static InstancedConfiguration sConf = Configuration.copyGlobal();

  @BeforeClass
  public static void classSetup() {
    sConf.set(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT, "1sec");
    sConf.set(PropertyKey.NETWORK_CONNECTION_SHUTDOWN_GRACEFUL_TIMEOUT, "500ms");
    sConf.set(PropertyKey.NETWORK_CONNECTION_SHUTDOWN_TIMEOUT, "1sec");
  }

  @After
  public void after() throws Exception {
    sConf = Configuration.copyGlobal();
  }

  @Test
  public void testEqualKeys() throws Exception {
    try (CloseableTestServer server = createServer()) {
      GrpcChannel conn1 = GrpcChannelPool.INSTANCE.acquireChannel(
          GrpcNetworkGroup.RPC, server.getConnectAddress(), sConf);
      GrpcChannel conn2 = GrpcChannelPool.INSTANCE.acquireChannel(
          GrpcNetworkGroup.RPC, server.getConnectAddress(), sConf);

      assertEquals(conn1, conn2);
    }
  }

  @Test
  public void testUnhealthyChannelRecreation() {

    // Not creating the corresponding server will ensure, the channels will never
    // be ready.
    GrpcServerAddress address = GrpcServerAddress.create(new InetSocketAddress("localhost", 1));

    GrpcChannel conn1 = GrpcChannelPool.INSTANCE.acquireChannel(
        GrpcNetworkGroup.RPC, address, sConf);
    GrpcChannel conn2 = GrpcChannelPool.INSTANCE.acquireChannel(
        GrpcNetworkGroup.RPC, address, sConf);

    assertNotEquals(conn1, conn2);
  }

  @Test
  public void testDifferentKeys() throws Exception {
    try (CloseableTestServer server1 = createServer();
        CloseableTestServer server2 = createServer()) {
      GrpcChannel conn1 = GrpcChannelPool.INSTANCE.acquireChannel(
          GrpcNetworkGroup.RPC, server1.getConnectAddress(), sConf);
      GrpcChannel conn2 = GrpcChannelPool.INSTANCE.acquireChannel(
          GrpcNetworkGroup.RPC, server2.getConnectAddress(), sConf);

      assertNotEquals(conn1, conn2);
    }
  }

  @Test
  public void testRoundRobin() throws Exception {
    int streamingGroupSize =
        sConf.getInt(PropertyKey.USER_NETWORK_STREAMING_MAX_CONNECTIONS);

    try (CloseableTestServer server = createServer()) {
      List<GrpcServerAddress> addresses = new ArrayList<>(streamingGroupSize);
      // Create channel keys.
      for (int i = 0; i < streamingGroupSize; i++) {
        addresses.add(server.getConnectAddress());
      }
      // Acquire connections.
      List<GrpcChannel> connections =
          addresses.stream()
              .map(address -> GrpcChannelPool.INSTANCE.acquireChannel(
                  GrpcNetworkGroup.STREAMING, address, sConf))
              .collect(Collectors.toList());

      // Validate all are different.
      Assert.assertEquals(streamingGroupSize, connections.stream().distinct().count());
    }
  }

  @Test
  public void testGroupSize() throws Exception {
    int streamingGroupSize =
        sConf.getInt(PropertyKey.USER_NETWORK_STREAMING_MAX_CONNECTIONS);
    int acquireCount = streamingGroupSize * 100;

    try (CloseableTestServer server = createServer()) {
      List<GrpcServerAddress> addresses = new ArrayList<>(acquireCount);
      // Create channel keys.
      for (int i = 0; i < acquireCount; i++) {
        addresses.add(server.getConnectAddress());
      }
      // Acquire connections.
      List<GrpcChannel> connections =
          addresses.stream()
              .map(address -> GrpcChannelPool.INSTANCE.acquireChannel(
                  GrpcNetworkGroup.STREAMING, address, sConf))
              .collect(Collectors.toList());

      // Validate all are different.
      Assert.assertEquals(streamingGroupSize, connections.stream().distinct().count());
    }
  }

  private CloseableTestServer createServer() throws Exception {
    InetSocketAddress bindAddress = new InetSocketAddress("0.0.0.0", 0);
    GrpcServer grpcServer = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create("localhost", bindAddress), sConf).build().start();
    return new CloseableTestServer(grpcServer);
  }

  private static class CloseableTestServer implements AutoCloseable {
    GrpcServer mServer;

    CloseableTestServer(GrpcServer server) {
      mServer = server;
    }

    GrpcServerAddress getConnectAddress() {
      return GrpcServerAddress.create(new InetSocketAddress("localhost", mServer.getBindPort()));
    }

    @Override
    public void close() throws Exception {
      mServer.shutdown();
    }
  }
}
