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

import alluxio.RpcUtils;
import alluxio.grpc.ClearMetricsPRequest;
import alluxio.grpc.ClearMetricsPResponse;
import alluxio.grpc.GetMetricsPOptions;
import alluxio.grpc.GetMetricsPResponse;
import alluxio.grpc.MetricsHeartbeatPRequest;
import alluxio.grpc.MetricsHeartbeatPResponse;
import alluxio.grpc.MetricsMasterClientServiceGrpc;
import alluxio.metrics.Metric;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a gRPC handler for metrics master RPCs invoked by an Alluxio client.
 */
@NotThreadSafe
public final class MetricsMasterClientServiceHandler
    extends MetricsMasterClientServiceGrpc.MetricsMasterClientServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(MetricsMasterClientServiceHandler.class);

  private final MetricsMaster mMetricsMaster;

  /**
   * @param metricsMaster {@link MetricsMaster} instance
   */
  public MetricsMasterClientServiceHandler(MetricsMaster metricsMaster) {
    Preconditions.checkNotNull(metricsMaster, "metricsMaster");
    mMetricsMaster = metricsMaster;
  }

  @Override
  public void clearMetrics(ClearMetricsPRequest request,
      StreamObserver<ClearMetricsPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mMetricsMaster.clearMetrics();
      return ClearMetricsPResponse.newBuilder().build();
    }, "clearMetrics", "request=%s", responseObserver, request);
  }

  @Override
  public void metricsHeartbeat(MetricsHeartbeatPRequest request,
      StreamObserver<MetricsHeartbeatPResponse> responseObserver) {
    RpcUtils.call(LOG,
        (RpcUtils.RpcCallableThrowsIOException<MetricsHeartbeatPResponse>) () -> {

          for (alluxio.grpc.ClientMetrics clientMetric :
              request.getOptions().getClientMetricsList()) {
            List<Metric> metrics = Lists.newArrayList();
            for (alluxio.grpc.Metric metric : clientMetric.getMetricsList()) {
              metrics.add(Metric.fromProto(metric));
            }
            mMetricsMaster.clientHeartbeat(
                clientMetric.getSource(), metrics);
          }
          return MetricsHeartbeatPResponse.getDefaultInstance();
        }, "metricsHeartbeat", "request=%s", responseObserver, request);
  }

  @Override
  public void getMetrics(GetMetricsPOptions options,
      StreamObserver<GetMetricsPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetMetricsPResponse>) () ->
        GetMetricsPResponse.newBuilder().putAllMetrics(mMetricsMaster.getMetrics()).build(),
        "getMetrics", "options=%s", responseObserver, options);
  }
}
