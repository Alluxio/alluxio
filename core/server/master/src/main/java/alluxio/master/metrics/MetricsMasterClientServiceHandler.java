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

package alluxio.master.metrics;

import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.RpcUtils.RpcCallableThrowsIOException;
import alluxio.exception.AlluxioException;
import alluxio.metrics.Metric;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.MetricsHeartbeatTOptions;
import alluxio.thrift.MetricsHeartbeatTResponse;
import alluxio.thrift.MetricsMasterClientService;

import com.google.common.base.Preconditions;
import jersey.repackaged.com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for metrics master RPCs invoked by an Alluxio client.
 */
@NotThreadSafe
public final class MetricsMasterClientServiceHandler implements MetricsMasterClientService.Iface {
  private static final Logger LOG =
      LoggerFactory.getLogger(MetricsMasterClientServiceHandler.class);

  private final MetricsMaster mMetricsMaster;

  MetricsMasterClientServiceHandler(MetricsMaster metricsMaster) {
    Preconditions.checkNotNull(metricsMaster, "metricsMaster");
    mMetricsMaster = metricsMaster;
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options)
      throws AlluxioTException, TException {
    return new GetServiceVersionTResponse(Constants.METRICS_MASTER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public MetricsHeartbeatTResponse metricsHeartbeat(final String clientId, final String hostname,
      final MetricsHeartbeatTOptions options) throws AlluxioTException, TException {
    return RpcUtils.call(LOG, new RpcCallableThrowsIOException<MetricsHeartbeatTResponse>() {
      @Override
      public MetricsHeartbeatTResponse call() throws AlluxioException, IOException {
        List<Metric> metrics = Lists.newArrayList();
        for (alluxio.thrift.Metric metric : options.getMetrics()) {
          Metric parsed =  Metric.from(metric);
          metrics.add(parsed);
        }
        mMetricsMaster.clientHeartbeat(clientId, hostname, metrics);
        return new MetricsHeartbeatTResponse();
      }

      @Override
      public String toString() {
        return String.format("clientHeartbeat: hostname=%s, contextId=%s, " + "options=%s",
            hostname, clientId, options);
      }
    });
  }
}
