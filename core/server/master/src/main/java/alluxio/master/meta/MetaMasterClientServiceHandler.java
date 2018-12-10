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

package alluxio.master.meta;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.grpc.BackupPOptions;
import alluxio.grpc.BackupPResponse;
import alluxio.grpc.GetConfigReportPOptions;
import alluxio.grpc.GetConfigReportPResponse;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.GetConfigurationPResponse;
import alluxio.grpc.GetMasterInfoPOptions;
import alluxio.grpc.GetMasterInfoPResponse;
import alluxio.grpc.GetMetricsPOptions;
import alluxio.grpc.GetMetricsPResponse;
import alluxio.grpc.MasterInfo;
import alluxio.grpc.MasterInfoField;
import alluxio.grpc.MetaMasterClientServiceGrpc;
import alluxio.metrics.MetricsSystem;
import alluxio.util.RpcUtilsNew;
import alluxio.wire.Address;
import alluxio.wire.BackupOptions;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class is a gRPC handler for meta master RPCs.
 */
public final class MetaMasterClientServiceHandler
    extends MetaMasterClientServiceGrpc.MetaMasterClientServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(MetaMasterClientServiceHandler.class);

  private final MetaMaster mMetaMaster;

  /**
   * @param metaMaster the Alluxio meta master
   */
  public MetaMasterClientServiceHandler(MetaMaster metaMaster) {
    mMetaMaster = metaMaster;
  }

  @Override
  public void backup(BackupPOptions options, StreamObserver<BackupPResponse> responseObserver) {

    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<BackupPResponse>) () -> {
      return mMetaMaster.backup(BackupOptions.fromProto(options)).toProto();
    }, "backup", "options=%s", responseObserver, options);
  }

  @Override
  public void getConfigReport(GetConfigReportPOptions options,
      StreamObserver<GetConfigReportPResponse> responseObserver) {
    RpcUtilsNew.call(LOG,
        (RpcUtilsNew.RpcCallableThrowsIOException<GetConfigReportPResponse>) () -> {

          return GetConfigReportPResponse.newBuilder()
              .setReport(mMetaMaster.getConfigCheckReport().toProto()).build();
        }, "getConfigReport", "options=%s", responseObserver, options);
  }

  @Override
  public void getConfiguration(GetConfigurationPOptions options,
      StreamObserver<GetConfigurationPResponse> responseObserver) {
    RpcUtilsNew.call(LOG,
        (RpcUtilsNew.RpcCallableThrowsIOException<GetConfigurationPResponse>) () -> {
          return GetConfigurationPResponse.newBuilder()
              .addAllConfigs(mMetaMaster.getConfiguration(options)).build();
        }, "getConfiguration", "options=%s", responseObserver, options);
  }

  @Override
  public void getMasterInfo(GetMasterInfoPOptions options,
      StreamObserver<GetMasterInfoPResponse> responseObserver) {
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<GetMasterInfoPResponse>) () -> {
      MasterInfo.Builder masterInfo = MasterInfo.newBuilder();
      for (MasterInfoField field : options.getFilterList() != null ? options.getFilterList()
          : Arrays.asList(MasterInfoField.values())) {
        switch (field) {
          case LEADER_MASTER_ADDRESS:
            masterInfo.setLeaderMasterAddress(mMetaMaster.getRpcAddress().toString());
            break;
          case MASTER_ADDRESSES:
            masterInfo.addAllMasterAddresses(mMetaMaster.getMasterAddresses().stream()
                .map(Address::toProto).collect(Collectors.toList()));
            break;
          case RPC_PORT:
            masterInfo.setRpcPort(mMetaMaster.getRpcAddress().getPort());
            break;
          case SAFE_MODE:
            masterInfo.setSafeMode(mMetaMaster.isInSafeMode());
            break;
          case START_TIME_MS:
            masterInfo.setStartTimeMs(mMetaMaster.getStartTimeMs());
            break;
          case UP_TIME_MS:
            masterInfo.setUpTimeMs(mMetaMaster.getUptimeMs());
            break;
          case VERSION:
            masterInfo.setVersion(RuntimeConstants.VERSION);
            break;
          case WEB_PORT:
            masterInfo.setWebPort(mMetaMaster.getWebPort());
            break;
          case WORKER_ADDRESSES:
            masterInfo.addAllWorkerAddresses(mMetaMaster.getWorkerAddresses().stream()
                .map(Address::toProto).collect(Collectors.toList()));
            break;
          case ZOOKEEPER_ADDRESSES:
            if (Configuration.isSet(PropertyKey.ZOOKEEPER_ADDRESS)) {
              masterInfo.addAllZookeeperAddresses(
                  Arrays.asList(Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS).split(",")));
            }
            break;
          default:
            LOG.warn("Unrecognized meta master info field: " + field);
        }
      }
      return GetMasterInfoPResponse.newBuilder().setMasterInfo(masterInfo).build();
    }, "getMasterInfo", "options=%s", responseObserver, options);
  }

  @Override
  public void getMetrics(GetMetricsPOptions options,
      StreamObserver<GetMetricsPResponse> responseObserver) {
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<GetMetricsPResponse>) () -> {

      MetricRegistry mr = MetricsSystem.METRIC_REGISTRY;
      Map<String, alluxio.grpc.MetricValue> metricsMap = new HashMap<>();

      for (Map.Entry<String, Counter> entry : mr.getCounters().entrySet()) {
        metricsMap.put(MetricsSystem.stripInstanceAndHost(entry.getKey()), alluxio.grpc.MetricValue
            .newBuilder().setLongValue(entry.getValue().getCount()).build());
      }

      for (Map.Entry<String, Gauge> entry : mr.getGauges().entrySet()) {
        Object value = entry.getValue().getValue();
        if (value instanceof Integer) {
          metricsMap.put(entry.getKey(), alluxio.grpc.MetricValue.newBuilder()
              .setLongValue(Long.valueOf((Integer) value)).build());
        } else if (value instanceof Long) {
          metricsMap.put(entry.getKey(), alluxio.grpc.MetricValue.newBuilder()
              .setLongValue(Long.valueOf((Integer) value)).build());
        } else if (value instanceof Double) {
          metricsMap.put(entry.getKey(),
              alluxio.grpc.MetricValue.newBuilder().setDoubleValue((Double) value).build());
        }
      }
      return GetMetricsPResponse.newBuilder().putAllMetrics(metricsMap).build();
    }, "getConfiguration", "options=%s", responseObserver, options);
  }
}
