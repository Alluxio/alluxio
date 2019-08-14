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

package alluxio.worker.job;

import alluxio.common.RpcPortHealthCheckClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.ServiceType;
import alluxio.retry.RetryPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

/**
 * JobWorkerHealthCheckClient check if worker is serving RPC.
 */
public class JobWorkerHealthCheckClient extends RpcPortHealthCheckClient {
  private static final Logger LOG = LoggerFactory.getLogger(JobWorkerHealthCheckClient.class);

  /**
   * Creates a worker health check client.
   *
   * @param jobWorkerAddress The potential job_worker address
   * @param retryPolicySupplier the retry policy supplier
   * @param alluxioConf Alluxio configuration
   */
  public JobWorkerHealthCheckClient(InetSocketAddress jobWorkerAddress,
      Supplier<RetryPolicy> retryPolicySupplier, AlluxioConfiguration alluxioConf) {
    super(jobWorkerAddress, ServiceType.UNKNOWN_SERVICE, retryPolicySupplier, alluxioConf);
  }
}
