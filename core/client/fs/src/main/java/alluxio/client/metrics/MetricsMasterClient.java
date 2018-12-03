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

package alluxio.client.metrics;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterClientConfig;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.thrift.AlluxioService.Client;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.Metric;
import alluxio.thrift.MetricsHeartbeatTOptions;
import alluxio.thrift.MetricsMasterClientService;
import alluxio.util.network.NetworkAddressUtils;

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A client to use for interacting with a metrics master.
 */
@ThreadSafe
public class MetricsMasterClient extends AbstractMasterClient {
  // No retry for metrics since they are best effort and automatically retried with the heartbeat.
  private static Supplier<RetryPolicy> defaultMetricsRetry() {
    return () -> new CountingRetry(0);
  }

  private MetricsMasterClientService.Client mClient = null;

  /**
   * Creates a new metrics master client.
   *
   * @param conf master client configuration
   */
  public MetricsMasterClient(MasterClientConfig conf) {
    super(conf, null, defaultMetricsRetry());
  }

  @Override
  protected Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.METRICS_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.METRICS_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = new MetricsMasterClientService.Client(mProtocol);
  }

  /**
   * The method the worker should periodically execute to heartbeat back to the master.
   *
   * @param metrics a list of client metrics
   */
  public synchronized void heartbeat(List<Metric> metrics) throws IOException {
    connect();
    try {
      mClient.metricsHeartbeat(MetricsSystem.getAppId(), NetworkAddressUtils.getClientHostName(),
          new MetricsHeartbeatTOptions(metrics));
    } catch (AlluxioTException e) {
      throw AlluxioStatusException.fromThrift(e);
    } catch (TException e) {
      throw new UnavailableException(e);
    }
  }
}
