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

package alluxio.worker;

import alluxio.Constants;
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
 * WorkerHealthCheckClient check if worker is serving RPC.
 */
public class WorkerHealthCheckClient implements HealthCheckClient {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerHealthCheckClient.class);

  private final InetSocketAddress mWorkerAddress;
  private final Supplier<RetryPolicy> mRetryPolicySupplier;

  /**
   * Creates a worker health check client.
   *
   * @param workerAddress The potential worker address
   * @param retryPolicySupplier the retry policy supplier
   */
  public WorkerHealthCheckClient(InetSocketAddress workerAddress,
      Supplier<RetryPolicy> retryPolicySupplier) {
    mWorkerAddress = workerAddress;
    mRetryPolicySupplier = retryPolicySupplier;
  }

  @Override
  public boolean isServing() {
    RetryPolicy retry = mRetryPolicySupplier.get();
    while (retry.attempt()) {
      try {
        LOG.debug("Checking whether {} is listening for RPCs", mWorkerAddress);
        NetworkAddressUtils.pingService(mWorkerAddress,
            Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_NAME);
        LOG.debug("Successfully connected to {}", mWorkerAddress);
        return true;
      } catch (ConnectionFailedException e) {
        LOG.debug("Failed to connect to {}", mWorkerAddress);
      } catch (UnauthenticatedException e) {
        throw new RuntimeException(e);
      }
    }
    return false;
  }
}
