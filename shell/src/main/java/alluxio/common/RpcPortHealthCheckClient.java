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

package alluxio.common;

import alluxio.HealthCheckClient;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.retry.RetryPolicy;
import alluxio.util.network.NetworkAddressUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

/**
 * RpcPortHealthCheckClient check if worker is serving RPC.
 */
public class RpcPortHealthCheckClient implements HealthCheckClient {
  private static final Logger LOG = LoggerFactory.getLogger(RpcPortHealthCheckClient.class);

  private final InetSocketAddress mNodeAddress;
  private final String mServiceName;
  private final Supplier<RetryPolicy> mRetryPolicySupplier;

  /**
   * Creates a worker health check client.
   *
   * @param nodeAddress The potential node address
   * @param serviceName The service name for node
   * @param retryPolicySupplier the retry policy supplier
   */
  public RpcPortHealthCheckClient(InetSocketAddress nodeAddress,
      String serviceName,
      Supplier<RetryPolicy> retryPolicySupplier) {
    mNodeAddress = nodeAddress;
    mServiceName = serviceName;
    mRetryPolicySupplier = retryPolicySupplier;
  }

  @Override
  public boolean isServing() {
    RetryPolicy retry = mRetryPolicySupplier.get();
    while (retry.attempt()) {
      try {
        LOG.debug("Checking whether {} is listening for RPCs", mNodeAddress);
        NetworkAddressUtils.pingService(mNodeAddress, mServiceName);
        LOG.debug("Successfully connected to {}", mNodeAddress);
        return true;
      } catch (ConnectionFailedException e) {
        LOG.debug("Failed to connect to {}", mNodeAddress);
      } catch (UnauthenticatedException e) {
        // TODO(ggezer) Revert after using NOSASL version checks in NetworkAddressUtils.pingService
        // throw new RuntimeException(e);
      }
    }
    return false;
  }
}
