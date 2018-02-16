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
import alluxio.exception.status.UnauthenticatedException;
import alluxio.retry.RetryPolicy;
import alluxio.security.authentication.TProtocols;
import alluxio.security.authentication.TransportProvider;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransportException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

/**
 * PollingWorkerHealthCheck finds the address of the worker by checking if the address is
 * serving RPC.
 */
public class PollingWorkerHealthCheck implements WorkerHealthCheck {
  private static final Logger LOG = LoggerFactory.getLogger(PollingWorkerHealthCheck.class);

  private final InetSocketAddress mWorkerAddress;
  private final Supplier<RetryPolicy> mRetryPolicySupplier;

  /**
   * @param workerAddress The potential worker address
   * @param retryPolicySupplier the retry policy supplier
   */
  public PollingWorkerHealthCheck(InetSocketAddress workerAddress,
                                  Supplier<RetryPolicy> retryPolicySupplier) {
    mWorkerAddress = workerAddress;
    mRetryPolicySupplier = retryPolicySupplier;
  }

  @Override
  public boolean isServing() {
    RetryPolicy retry = mRetryPolicySupplier.get();
    do {
      try {
        LOG.debug("Checking whether {} is listening for RPCs", mWorkerAddress);
        pingWorkerService(mWorkerAddress);
        LOG.debug("Successfully connected to {}", mWorkerAddress);
        return true;
      } catch (TTransportException e) {
        LOG.debug("Failed to connect to {}", mWorkerAddress);
      } catch (UnauthenticatedException e) {
        throw new RuntimeException(e);
      }
    } while (retry.attemptRetry());
    return false;
  }

  private void pingWorkerService(InetSocketAddress address)
          throws UnauthenticatedException, TTransportException {
    TransportProvider transportProvider = TransportProvider.Factory.create();
    TProtocol protocol = TProtocols.createProtocol(transportProvider.getClientTransport(address),
            Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_NAME);
    protocol.getTransport().open();
    protocol.getTransport().close();
  }

}
