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

package alluxio.hub.manager.process;

import alluxio.conf.ServerConfiguration;
import alluxio.grpc.GetServiceVersionPRequest;
import alluxio.grpc.ServiceType;
import alluxio.grpc.ServiceVersionClientServiceGrpc;
import alluxio.hub.common.RpcClient;
import alluxio.hub.manager.rpc.ManagerRpcServer;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.retry.RetryPolicy;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Alluxio Hub Manager monitor for inquiring about the Hub Manager's service availability.
 */
public final class ManagerProcessMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(ManagerProcessMonitor.class);

  /**
   * Starts the Alluxio master monitor.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    RetryPolicy policy = new ExponentialBackoffRetry(50, 1000,
            130); // Max time ~2 min
    InetSocketAddress addr = ManagerRpcServer.getConnectAddress(ServerConfiguration.global());
    if (addr.isUnresolved()) {
      addr = new InetSocketAddress(addr.getHostName(), addr.getPort());
    }
    try {
      pingService(addr, policy, 5000);
    } catch (Exception e) {
      LOG.error("Failed to connect to Hub Manager RPC server @" + addr);
      System.exit(1);
    }
    System.exit(0);
  }

  /**
   * Attempts to ping the version service of the RPC server.
   *
   * @param addr The address to connect to
   * @param policy the retry policy
   * @param timeoutMs the timeout to wait for a ping response
   * @throws Exception if the client can't connect
   */
  static void pingService(InetSocketAddress addr, RetryPolicy policy, long timeoutMs)
          throws Exception {
    ManagedChannel channel = NettyChannelBuilder.forAddress(addr).build();
    RpcClient<ServiceVersionClientServiceGrpc.ServiceVersionClientServiceBlockingStub>
            versionClient = new RpcClient<>(ServerConfiguration.global(), addr,
                ServiceVersionClientServiceGrpc::newBlockingStub,
                () -> ExponentialTimeBoundedRetry.builder().withSkipInitialSleep()
                        .withMaxDuration(Duration.ofMillis(timeoutMs)).build());
    try {
      while (policy.attempt()) {
        try {
          versionClient.get().withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).getServiceVersion(
              GetServiceVersionPRequest.newBuilder().setServiceType(ServiceType.UNKNOWN_SERVICE)
                      .build());
          return;
        } catch (Throwable t) {
          LOG.info("Failed to reach version service", t);
        }
      }
    } finally {
      channel.shutdown();
      channel.awaitTermination(3, TimeUnit.SECONDS);
    }
    throw new Exception("Failed to reach the version service after "
        + policy.getAttemptCount() + " attempts");
  }

  private ManagerProcessMonitor() {} // prevent
  // instantiation
}
