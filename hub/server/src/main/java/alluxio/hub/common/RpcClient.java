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

package alluxio.hub.common;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServerAddress;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authentication.AuthType;
import alluxio.security.user.ServerUserState;

import io.grpc.Channel;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A wrapper around a particular client to a gRPC service that will automatically create and
 * connect a channel to the desired endpoint. After creating this class, typical usage will be
 *
 * <tt>
 *   RpcClient c = new RpcClient(conf, address, service::newBlockingStub)
 *   c.get().makeRpc(argument)
 * </tt>
 *
 * The class tracks the underlying channel and if it becomes closed or unhealthy will
 * automatically create and connect with a new one.
 *
 * @param <T> the type of client to expose
 */
public class RpcClient<T extends io.grpc.stub.AbstractBlockingStub<T>> implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RpcClient.class);

  private static final Supplier<RetryPolicy> POLICY_SUPPLIER =
      () -> ExponentialTimeBoundedRetry.builder()
      .withInitialSleep(Duration.ofMillis(0))
      .withMaxSleep(Duration.ofMillis(2000))
      .withSkipInitialSleep()
      .withMaxDuration(Duration.ofMinutes(2))
      .build();

  /**
   * The currently open channel to the desired target.
   *
   * We use {@link Channel} here instead of {@link GrpcChannel} because it allows for building
   * unit tests around the gRPC services without needing to create an actual server. In this
   * class we'll perform {@code instanceof} checks in order to call methods for
   * {@link GrpcChannel} specific pieces of code.
   */
  private Channel mChannel;
  private T mClient;
  private final Function<Channel, T> mClientFactory;
  private final ChannelSupplier mChannelFactory;
  private final InetSocketAddress mAddr;

  /**
   * Create a new instance of the {@link RpcClient}.
   *
   * @param conf the configuration used to make the connection
   * @param connectAddress the address to connect to
   * @param channelFactory the factory method used create a new gRPC client around the channel
   * @param policySupplier the a supplier of policies to use when attempting to create new channels
   */
  public RpcClient(AlluxioConfiguration conf, InetSocketAddress connectAddress,
      Function<Channel, T> channelFactory, Supplier<RetryPolicy> policySupplier) {
    this(connectAddress, channelFactory, (addr) -> {
      AlluxioStatusException last = null;
      RetryPolicy policy = policySupplier.get();
      while (policy.attempt()) {
        try {
          return createChannel(addr, conf);
        } catch (AlluxioStatusException e) {
          last = e;
        }
      }
      if (last != null) {
        throw last;
      } else {
        throw new AlluxioStatusException(Status.UNKNOWN
            .withDescription("Failed to create a channel connecting to " + connectAddress));
      }
    });
  }

  /**
   * Creates a gRPC channel that uses NOSASL for security authentication type.
   *
   * @param addr the address to connect to
   * @param conf the configuration used to make the connection
   * @return new instance of {@link GrpcChannel}
   * @throws AlluxioStatusException
   */
  public static GrpcChannel createChannel(InetSocketAddress addr, AlluxioConfiguration conf)
      throws AlluxioStatusException {
    InstancedConfiguration modifiedConfig = InstancedConfiguration.defaults();
    Map<String, Object> properties = new HashMap<>(conf.toMap());
    properties.put(PropertyKey.SECURITY_AUTHENTICATION_TYPE.getName(), AuthType.NOSASL);
    modifiedConfig.merge(properties, Source.RUNTIME);
    LOG.info("Auth type = {}", modifiedConfig.get(PropertyKey.SECURITY_AUTHENTICATION_TYPE));
    GrpcChannelBuilder builder = GrpcChannelBuilder
        .newBuilder(GrpcServerAddress.create(addr), modifiedConfig);
    return builder.setSubject(ServerUserState.global().getSubject()).build();
  }

  /**
   * Create a new RPC client with the desired channel factory. Use this method for testing.
   *
   * @param factory the client factory
   * @param channelFactory the channel factory
   */
  RpcClient(InetSocketAddress addr, Function<Channel, T> factory, ChannelSupplier channelFactory) {
    mClientFactory = factory;
    mChannelFactory = channelFactory;
    mAddr = addr;
  }

  private synchronized void beforeRpc() throws AlluxioStatusException {
    if (mChannel == null
        || (mChannel instanceof GrpcChannel && (((GrpcChannel) mChannel).isShutdown()
        || !((GrpcChannel) mChannel).isHealthy()))) {
      // Perform this check because it could have been shutdown or unhealthy.
      // If it's not shut down, then we should shut down the old channel before creating a new one.
      if (mChannel instanceof GrpcChannel && !((GrpcChannel) mChannel).isShutdown()) {
        ((GrpcChannel) mChannel).shutdown();
      }
      mChannel = mChannelFactory.get(mAddr);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Created new channel connecting to {}", mChannel.authority());
      }
      mClient = mClientFactory.apply(mChannel);
    }
  }

  /**
   * Create if necessary a new client if necessary, then return the client.
   *
   * @return the currently connected client
   * @throws AlluxioStatusException if a connection to the endpoint can't be made
   */
  public synchronized T get() throws AlluxioStatusException {
    beforeRpc();
    return mClient;
  }

  /**
   * @return the address that this RPC client connects to
   */
  public InetSocketAddress getAddress() {
    return mAddr;
  }

  @Override
  public synchronized void close() {
    if (mChannel instanceof GrpcChannel) {
      ((GrpcChannel) mChannel).shutdown();
    }
  }
}
