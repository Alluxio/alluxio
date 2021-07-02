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
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.ServiceType;
import alluxio.retry.RetryPolicy;
import alluxio.security.user.UserState;
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
  private final ServiceType mServiceType;
  private final Supplier<RetryPolicy> mRetryPolicySupplier;
  private final AlluxioConfiguration mConf;
  private final UserState mUserState;

  /**
   * Creates a worker health check client.
   *
   * @param nodeAddress The potential node address
   * @param serviceType The type of service
   * @param retryPolicySupplier the retry policy supplier
   * @param alluxioConf Alluxio configuration
   */
  public RpcPortHealthCheckClient(InetSocketAddress nodeAddress,
      ServiceType serviceType,
      Supplier<RetryPolicy> retryPolicySupplier,
      AlluxioConfiguration alluxioConf) {
    mNodeAddress = nodeAddress;
    mServiceType = serviceType;
    mRetryPolicySupplier = retryPolicySupplier;
    mConf = alluxioConf;
    mUserState = UserState.Factory.create(mConf);
  }

  @Override
  public boolean isServing() {
    RetryPolicy retry = mRetryPolicySupplier.get();
    while (retry.attempt()) {
      try {
        LOG.debug("Checking whether {} is listening for RPCs", mNodeAddress);
        NetworkAddressUtils.pingService(mNodeAddress, mServiceType, mConf, mUserState);
        LOG.debug("Successfully connected to {}", mNodeAddress);
        return true;
      } catch (UnavailableException e) {
        LOG.debug("Failed to connect to {}", mNodeAddress);
      } catch (AlluxioStatusException e) {
        throw new RuntimeException(e);
      }
    }
    return false;
  }
}
