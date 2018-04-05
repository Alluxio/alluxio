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

package alluxio.master;

import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.RuntimeConstants;
import alluxio.metrics.MetricsSystem;
import alluxio.thrift.GetMasterInfoTOptions;
import alluxio.thrift.GetMasterInfoTResponse;
import alluxio.thrift.GetMetricsTOptions;
import alluxio.thrift.GetMetricsTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.MasterInfo;
import alluxio.thrift.MasterInfoField;
import alluxio.thrift.MetaMasterClientService;
import alluxio.wire.MetricValue;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is a Thrift handler for meta master RPCs.
 */
public final class MetaMasterClientServiceHandler implements MetaMasterClientService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(MetaMasterClientServiceHandler.class);

  private final MasterProcess mMasterProcess;

  /**
   * @param masterProcess the Alluxio master process
   */
  MetaMasterClientServiceHandler(MasterProcess masterProcess) {
    mMasterProcess = masterProcess;
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.META_MASTER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public GetMasterInfoTResponse getMasterInfo(final GetMasterInfoTOptions options)
      throws TException {
    return RpcUtils.call(LOG, (RpcUtils.RpcCallable<GetMasterInfoTResponse>) () -> {
      MasterInfo info = new alluxio.thrift.MasterInfo();
      for (MasterInfoField field : options.getFilter() != null ? options.getFilter()
          : Arrays.asList(MasterInfoField.values())) {
        switch (field) {
          case MASTER_ADDRESS:
            info.setMasterAddress(mMasterProcess.getRpcAddress().toString());
            break;
          case RPC_PORT:
            info.setRpcPort(mMasterProcess.getRpcAddress().getPort());
            break;
          case SAFE_MODE:
            info.setSafeMode(mMasterProcess.isInSafeMode());
            break;
          case START_TIME_MS:
            info.setStartTimeMs(mMasterProcess.getStartTimeMs());
            break;
          case UP_TIME_MS:
            info.setUpTimeMs(mMasterProcess.getUptimeMs());
            break;
          case VERSION:
            info.setVersion(RuntimeConstants.VERSION);
            break;
          case WEB_PORT:
            info.setWebPort(mMasterProcess.getWebAddress().getPort());
            break;
          default:
            LOG.warn("Unrecognized meta master info field: " + field);
        }
      }
      return new GetMasterInfoTResponse(info);
    });
  }

  @Override
  public GetMetricsTResponse getMetrics(final GetMetricsTOptions options)
      throws TException {
    return RpcUtils.call(LOG, (RpcUtils.RpcCallable<GetMetricsTResponse>) () -> {
      MetricRegistry mr = MetricsSystem.METRIC_REGISTRY;
      Map<String, alluxio.thrift.MetricValue> metricsMap = new HashMap<>();

      for (Map.Entry<String, Counter> entry : mr.getCounters().entrySet()) {
        metricsMap.put(MetricsSystem.stripInstanceAndHost(entry.getKey()),
            new MetricValue().setLongValue(entry.getValue().getCount()).toThrift());
      }

      for (Map.Entry<String, Gauge> entry : mr.getGauges().entrySet()) {
        Object value = entry.getValue().getValue();
        if (value instanceof Integer) {
          metricsMap.put(entry.getKey(),
              new MetricValue().setLongValue(Long.valueOf((Integer) value)).toThrift());
        } else if (value instanceof Long) {
          metricsMap.put(entry.getKey(),
              new MetricValue().setLongValue((Long) value).toThrift());
        } else if (value instanceof Double) {
          metricsMap.put(entry.getKey(),
              new MetricValue().setDoubleValue((Double) value).toThrift());
        }
      }
      return new GetMetricsTResponse(metricsMap);
    });
  }
}
