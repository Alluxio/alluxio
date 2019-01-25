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

import static org.junit.Assert.assertTrue;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.SleepUtils;

import io.grpc.ManagedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link GrpcManagedChannelPool}.
 */
public final class GrpcManagedChannelPoolTest {

  private static InstancedConfiguration sConf = ConfigurationTestUtils.defaults();

  @BeforeClass
  public static void classSetup() {
    sConf.set(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT_MS, "1sec");
  }

  @After
  public void after() throws Exception {
    sConf = ConfigurationTestUtils.defaults();
  }

  @Test
  public void testEqualKeys() throws Exception {

    GrpcManagedChannelPool.ChannelKey key1 = GrpcManagedChannelPool.ChannelKey.create();
    GrpcManagedChannelPool.ChannelKey key2 = GrpcManagedChannelPool.ChannelKey.create();

    GrpcServer server1 =
        GrpcServerBuilder.forAddress(new InetSocketAddress("0.0.0.0", 0), sConf).build().start();
    SocketAddress address = new InetSocketAddress("localhost", server1.getBindPort());

    key1.setAddress(address);
    key2.setAddress(address);

    ManagedChannel channel1 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key1);
    ManagedChannel channel2 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key2);

    assertTrue(channel1 == channel2);

    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key1);
    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key2);
    server1.shutdown();
  }

  @Test
  public void testUnhealthyChannelRecreation() throws Exception {

    GrpcManagedChannelPool.ChannelKey key1 = GrpcManagedChannelPool.ChannelKey.create();
    GrpcManagedChannelPool.ChannelKey key2 = GrpcManagedChannelPool.ChannelKey.create();

    // Not creating the coresponding server will ensure, the channels will never
    // be ready.
    SocketAddress address = new InetSocketAddress("localhost", 1);

    key1.setAddress(address);
    key2.setAddress(address);

    ManagedChannel channel1 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key1);
    ManagedChannel channel2 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key2);

    assertTrue(channel1 != channel2);

    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key1);
    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key2);
  }

  @Test
  public void testEqualKeysComplex() throws Exception {
    GrpcManagedChannelPool.ChannelKey key1 = GrpcManagedChannelPool.ChannelKey.create();
    GrpcManagedChannelPool.ChannelKey key2 = GrpcManagedChannelPool.ChannelKey.create();

    GrpcServer server1 =
            GrpcServerBuilder.forAddress(new InetSocketAddress("0.0.0.0", 0), sConf).build().start();

    SocketAddress address = new InetSocketAddress("localhost", server1.getBindPort());

    key1.setAddress(address);
    key2.setAddress(address);

    key1.setFlowControlWindow(100);
    key2.setFlowControlWindow(100);

    key1.setMaxInboundMessageSize(100);
    key2.setMaxInboundMessageSize(100);

    key1.setKeepAliveTime(100, TimeUnit.MINUTES);
    key2.setKeepAliveTime(100, TimeUnit.MINUTES);

    key1.setKeepAliveTimeout(100, TimeUnit.MINUTES);
    key2.setKeepAliveTimeout(100, TimeUnit.MINUTES);

    ManagedChannel channel1 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key1);
    ManagedChannel channel2 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key2);

    assertTrue(channel1 == channel2);

    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key1);
    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key2);
    server1.shutdown();
  }

  @Test
  public void testNotEqualKeys() throws Exception {
    GrpcManagedChannelPool.ChannelKey key1 = GrpcManagedChannelPool.ChannelKey.create();
    GrpcManagedChannelPool.ChannelKey key2 = GrpcManagedChannelPool.ChannelKey.create();

    GrpcServer server1 =
            GrpcServerBuilder.forAddress(new InetSocketAddress("0.0.0.0", 0), sConf).build().start();
    GrpcServer server2 =
            GrpcServerBuilder.forAddress(new InetSocketAddress("0.0.0.0", 0), sConf).build().start();

    SocketAddress address1 = new InetSocketAddress("localhost", server1.getBindPort());
    SocketAddress address2 = new InetSocketAddress("localhost", server2.getBindPort());

    key1.setAddress(address1);
    key2.setAddress(address2);

    ManagedChannel channel1 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key1);
    ManagedChannel channel2 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key2);

    assertTrue(channel1 != channel2);

    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key1);
    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key2);
    server2.shutdown();
    server2.shutdown();
  }

  @Test
  public void testEqualKeysNoPooling() throws Exception {
    GrpcManagedChannelPool.ChannelKey key1 = GrpcManagedChannelPool.ChannelKey.create()
        .setPoolingStrategy(GrpcManagedChannelPool.PoolingStrategy.DISABLED);
    GrpcManagedChannelPool.ChannelKey key2 = GrpcManagedChannelPool.ChannelKey.create()
        .setPoolingStrategy(GrpcManagedChannelPool.PoolingStrategy.DISABLED);

    GrpcServer server1 =
            GrpcServerBuilder.forAddress(new InetSocketAddress("0.0.0.0", 0), sConf).build().start();

    SocketAddress address = new InetSocketAddress("localhost", server1.getBindPort());

    key1.setAddress(address);
    key2.setAddress(address);

    ManagedChannel channel1 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key1);
    ManagedChannel channel2 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key2);

    assertTrue(channel1 != channel2);

    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key1);
    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key2);
    server1.shutdown();
  }
}
