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

package alluxio.proxy;

import alluxio.HealthCheckClient;
import alluxio.retry.RetryPolicy;
import alluxio.util.network.NetworkAddressUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

/**
 * ProxyHealthCheckClient check if the proxy server is serving requests.
 */
public class ProxyHealthCheckClient implements HealthCheckClient {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyHealthCheckClient.class);

  private final InetSocketAddress mProxyAddress;
  private final Supplier<RetryPolicy> mRetryPolicySupplier;

  /**
   * Creates a proxy health check client.
   *
   * @param proxyAddress The potential proxy address
   * @param retryPolicySupplier the retry policy supplier
   */
  public ProxyHealthCheckClient(InetSocketAddress proxyAddress,
      Supplier<RetryPolicy> retryPolicySupplier) {
    mProxyAddress = proxyAddress;
    mRetryPolicySupplier = retryPolicySupplier;
  }

  @Override
  public boolean isServing() {
    RetryPolicy retry = mRetryPolicySupplier.get();
    while (retry.attempt()) {
      LOG.debug("Checking whether {} is listening", mProxyAddress);
      boolean connected = NetworkAddressUtils.isServing(mProxyAddress.getHostName(),
              mProxyAddress.getPort());
      if (!connected) {
        LOG.debug("Failed to connect to {}", mProxyAddress);
        continue;
      }
      LOG.debug("Successfully connected to {}", mProxyAddress);
      return true;
    }
    return false;
  }
}
