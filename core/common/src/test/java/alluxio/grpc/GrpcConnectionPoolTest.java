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

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.user.UserState;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link GrpcConnectionPool}.
 */
public final class GrpcConnectionPoolTest {

  private static InstancedConfiguration sConf = ConfigurationTestUtils.defaults();

  @BeforeClass
  public static void classSetup() {
    sConf.set(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT, "1sec");
    sConf.set(PropertyKey.NETWORK_CONNECTION_SHUTDOWN_GRACEFUL_TIMEOUT, "500ms");
    sConf.set(PropertyKey.NETWORK_CONNECTION_SHUTDOWN_TIMEOUT, "1sec");
  }

  @After
  public void after() throws Exception {
    sConf = ConfigurationTestUtils.defaults();
  }

  @Test
  public void testEqualKeys() throws Exception {
    try (CloseableTestServer server = createServer()) {
      GrpcChannelKey key1 =
          GrpcChannelKey.create(sConf).setServerAddress(server.getConnectAddress());
      GrpcChannelKey key2 =
          GrpcChannelKey.create(sConf).setServerAddress(server.getConnectAddress());

      GrpcConnection conn1 = GrpcConnectionPool.INSTANCE.acquireConnection(key1, sConf);
      GrpcConnection conn2 = GrpcConnectionPool.INSTANCE.acquireConnection(key2, sConf);

      assertEquals(conn1, conn2);
    }
  }

  @Test
  public void testUnhealthyChannelRecreation() throws Exception {
    GrpcChannelKey key = GrpcChannelKey.create(sConf);

    // Not creating the corresponding server will ensure, the channels will never
    // be ready.
    GrpcServerAddress address = GrpcServerAddress.create(new InetSocketAddress("localhost", 1));
    key.setServerAddress(address);

    GrpcConnection conn1 = GrpcConnectionPool.INSTANCE.acquireConnection(key, sConf);
    GrpcConnection conn2 = GrpcConnectionPool.INSTANCE.acquireConnection(key, sConf);

    assertNotEquals(conn1, conn2);
  }

  @Test
  public void testDifferentKeys() throws Exception {
    try (CloseableTestServer server1 = createServer();
        CloseableTestServer server2 = createServer()) {
      GrpcChannelKey key1 =
          GrpcChannelKey.create(sConf).setServerAddress(server1.getConnectAddress());
      GrpcChannelKey key2 =
          GrpcChannelKey.create(sConf).setServerAddress(server2.getConnectAddress());

      GrpcConnection conn1 = GrpcConnectionPool.INSTANCE.acquireConnection(key1, sConf);
      GrpcConnection conn2 = GrpcConnectionPool.INSTANCE.acquireConnection(key2, sConf);

      assertNotEquals(conn1, conn2);
    }
  }

  @Test
  public void testRoundRobin() throws Exception {
    int streamingGroupSize =
        sConf.getInt(PropertyKey.USER_NETWORK_STREAMING_MAX_CONNECTIONS);

    try (CloseableTestServer server = createServer()) {
      List<GrpcChannelKey> keys = new ArrayList(streamingGroupSize);
      // Create channel keys.
      for (int i = 0; i < streamingGroupSize; i++) {
        keys.add(GrpcChannelKey.create(sConf).setNetworkGroup(GrpcNetworkGroup.STREAMING)
            .setServerAddress(server.getConnectAddress()));
      }
      // Acquire connections.
      List<GrpcConnection> connections =
          keys.stream().map((key) -> GrpcConnectionPool.INSTANCE.acquireConnection(key, sConf))
              .collect(Collectors.toList());

      // Validate all are different.
      Assert.assertEquals(streamingGroupSize,
          connections.stream().distinct().collect(Collectors.toList()).size());
    }
  }

  @Test
  public void testGroupSize() throws Exception {
    int streamingGroupSize =
        sConf.getInt(PropertyKey.USER_NETWORK_STREAMING_MAX_CONNECTIONS);
    int acquireCount = streamingGroupSize * 100;

    try (CloseableTestServer server = createServer()) {
      List<GrpcChannelKey> keys = new ArrayList(acquireCount);
      // Create channel keys.
      for (int i = 0; i < acquireCount; i++) {
        keys.add(GrpcChannelKey.create(sConf).setNetworkGroup(GrpcNetworkGroup.STREAMING)
            .setServerAddress(server.getConnectAddress()));
      }
      // Acquire connections.
      List<GrpcConnection> connections =
          keys.stream().map((key) -> GrpcConnectionPool.INSTANCE.acquireConnection(key, sConf))
              .collect(Collectors.toList());

      // Validate all are different.
      Assert.assertEquals(streamingGroupSize,
          connections.stream().distinct().collect(Collectors.toList()).size());
    }
  }

  private CloseableTestServer createServer() throws Exception {
    InetSocketAddress bindAddress = new InetSocketAddress("0.0.0.0", 0);
    UserState us = UserState.Factory.create(sConf);
    GrpcServer grpcServer = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create("localhost", bindAddress), sConf, us).build().start();
    return new CloseableTestServer(grpcServer);
  }

  private class CloseableTestServer implements AutoCloseable {
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
