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

package alluxio.master.job;

import alluxio.common.RpcPortHealthCheckClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.ServiceType;
import alluxio.retry.RetryPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

/**
 * JobMasterHealthCheckClient check if job_master is serving RPC.
 */
public class JobMasterRpcHealthCheckClient extends RpcPortHealthCheckClient {
  private static final Logger LOG = LoggerFactory.getLogger(JobMasterRpcHealthCheckClient.class);

  /**
   * Creates a job_master health check client.
   *
   * @param jobMasterAddress The potential job_master address
   * @param retryPolicySupplier the retry policy supplier
   * @param alluxioConf Alluxio configuration
   */
  public JobMasterRpcHealthCheckClient(InetSocketAddress jobMasterAddress,
      Supplier<RetryPolicy> retryPolicySupplier, AlluxioConfiguration alluxioConf) {
    super(jobMasterAddress, ServiceType.JOB_MASTER_CLIENT_SERVICE, retryPolicySupplier,
        alluxioConf);
  }
}
