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
import alluxio.security.user.UserState;

import io.grpc.ManagedChannel;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link GrpcManagedChannelPool}.
 */
public final class GrpcManagedChannelPoolTest {

  private static InstancedConfiguration sConf = ConfigurationTestUtils.defaults();
  private static final long SHUTDOWN_TIMEOUT =
      sConf.getMs(PropertyKey.NETWORK_CONNECTION_SHUTDOWN_TIMEOUT);
  private static final long HEALTH_CHECK_TIMEOUT =
      sConf.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);

  @BeforeClass
  public static void classSetup() {
    sConf.set(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT, "1sec");
  }

  @After
  public void after() throws Exception {
    sConf = ConfigurationTestUtils.defaults();
  }

  @Test
  public void testEqualKeys() throws Exception {
    GrpcChannelKey key1 = GrpcChannelKey.create(sConf);
    GrpcChannelKey key2 = GrpcChannelKey.create(sConf);

    InetSocketAddress bindAddress =  new InetSocketAddress("0.0.0.0", 0);

    UserState us = UserState.Factory.create(sConf);
    GrpcServer server1 = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create("localhost", bindAddress), sConf, us).build().start();

    GrpcServerAddress address1 =
        GrpcServerAddress.create(new InetSocketAddress("localhost", server1.getBindPort()));
    GrpcServerAddress address2 =
        GrpcServerAddress.create(new InetSocketAddress("localhost", server1.getBindPort()));

    key1.setServerAddress(address1);
    key2.setServerAddress(address2);

    ManagedChannel channel1 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key1,
        HEALTH_CHECK_TIMEOUT, SHUTDOWN_TIMEOUT);
    ManagedChannel channel2 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key2,
        HEALTH_CHECK_TIMEOUT, SHUTDOWN_TIMEOUT);

    assertTrue(channel1 == channel2);

    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key1, SHUTDOWN_TIMEOUT);
    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key2, SHUTDOWN_TIMEOUT);
    server1.shutdown();
  }

  @Test
  public void testUnhealthyChannelRecreation() throws Exception {

    GrpcChannelKey key1 = GrpcChannelKey.create(sConf);
    GrpcChannelKey key2 = GrpcChannelKey.create(sConf);

    // Not creating the coresponding server will ensure, the channels will never
    // be ready.
    GrpcServerAddress address = GrpcServerAddress.create(new InetSocketAddress("localhost", 1));

    key1.setServerAddress(address);
    key2.setServerAddress(address);

    ManagedChannel channel1 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key1,
        HEALTH_CHECK_TIMEOUT, SHUTDOWN_TIMEOUT);
    ManagedChannel channel2 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key2,
        HEALTH_CHECK_TIMEOUT, SHUTDOWN_TIMEOUT);

    assertTrue(channel1 != channel2);

    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key1, SHUTDOWN_TIMEOUT);
    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key2, SHUTDOWN_TIMEOUT);
  }

  @Test
  public void testEqualKeysComplex() throws Exception {
    GrpcChannelKey key1 = GrpcChannelKey.create(sConf);
    GrpcChannelKey key2 = GrpcChannelKey.create(sConf);

    InetSocketAddress bindAddress =  new InetSocketAddress("0.0.0.0", 0);

    UserState us = UserState.Factory.create(sConf);
    GrpcServer server1 = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create("localhost", bindAddress), sConf, us).build().start();

    GrpcServerAddress address =
        GrpcServerAddress.create(new InetSocketAddress("localhost", server1.getBindPort()));

    key1.setServerAddress(address);
    key2.setServerAddress(address);

    key1.setFlowControlWindow(100);
    key2.setFlowControlWindow(100);

    key1.setMaxInboundMessageSize(100);
    key2.setMaxInboundMessageSize(100);

    key1.setKeepAliveTime(100, TimeUnit.MINUTES);
    key2.setKeepAliveTime(100, TimeUnit.MINUTES);

    key1.setKeepAliveTimeout(100, TimeUnit.MINUTES);
    key2.setKeepAliveTimeout(100, TimeUnit.MINUTES);

    ManagedChannel channel1 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key1,
        HEALTH_CHECK_TIMEOUT, SHUTDOWN_TIMEOUT);
    ManagedChannel channel2 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key2,
        HEALTH_CHECK_TIMEOUT, SHUTDOWN_TIMEOUT);

    assertTrue(channel1 == channel2);

    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key1, SHUTDOWN_TIMEOUT);
    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key2, SHUTDOWN_TIMEOUT);
    server1.shutdown();
  }

  @Test
  public void testNotEqualKeys() throws Exception {
    GrpcChannelKey key1 = GrpcChannelKey.create(sConf);
    GrpcChannelKey key2 = GrpcChannelKey.create(sConf);

    InetSocketAddress bindAddress =  new InetSocketAddress("0.0.0.0", 0);

    UserState us = UserState.Factory.create(sConf);
    GrpcServer server1 = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create("localhost", bindAddress), sConf, us).build().start();
    GrpcServer server2 = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create("localhost", bindAddress), sConf, us).build().start();

    GrpcServerAddress address1 =
        GrpcServerAddress.create(new InetSocketAddress("localhost", server1.getBindPort()));
    GrpcServerAddress address2 =
        GrpcServerAddress.create(new InetSocketAddress("localhost", server2.getBindPort()));

    key1.setServerAddress(address1);
    key2.setServerAddress(address2);

    ManagedChannel channel1 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key1,
        HEALTH_CHECK_TIMEOUT, SHUTDOWN_TIMEOUT);
    ManagedChannel channel2 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key2,
        HEALTH_CHECK_TIMEOUT, SHUTDOWN_TIMEOUT);

    assertTrue(channel1 != channel2);

    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key1, SHUTDOWN_TIMEOUT);
    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key2, SHUTDOWN_TIMEOUT);
    server2.shutdown();
    server2.shutdown();
  }

  @Test
  public void testEqualKeysNoPooling() throws Exception {
    GrpcChannelKey key1 = GrpcChannelKey.create(sConf)
        .setPoolingStrategy(GrpcChannelKey.PoolingStrategy.DISABLED);
    GrpcChannelKey key2 = GrpcChannelKey.create(sConf)
        .setPoolingStrategy(GrpcChannelKey.PoolingStrategy.DISABLED);

    InetSocketAddress bindAddress = new InetSocketAddress("0.0.0.0", 0);

    UserState us = UserState.Factory.create(sConf);
    GrpcServer server1 = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create("localhost", bindAddress), sConf, us).build().start();

    GrpcServerAddress address =
        GrpcServerAddress.create(new InetSocketAddress("localhost", server1.getBindPort()));

    key1.setServerAddress(address);
    key2.setServerAddress(address);

    ManagedChannel channel1 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key1,
        HEALTH_CHECK_TIMEOUT, SHUTDOWN_TIMEOUT);
    ManagedChannel channel2 = GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(key2,
        HEALTH_CHECK_TIMEOUT, SHUTDOWN_TIMEOUT);

    assertTrue(channel1 != channel2);

    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key1, SHUTDOWN_TIMEOUT);
    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(key2, SHUTDOWN_TIMEOUT);

    server1.shutdown();
  }
}
