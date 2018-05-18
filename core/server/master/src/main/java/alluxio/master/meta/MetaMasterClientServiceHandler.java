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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RpcUtils;
import alluxio.RuntimeConstants;
import alluxio.metrics.MetricsSystem;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.GetConfigurationTOptions;
import alluxio.thrift.GetConfigurationTResponse;
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
import java.util.stream.Collectors;

/**
 * This class is a Thrift handler for meta master RPCs.
 */
public final class MetaMasterClientServiceHandler implements MetaMasterClientService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(MetaMasterClientServiceHandler.class);

  private final MetaMaster mMetaMaster;

  /**
   * @param metaMaster the Alluxio meta master
   */
  public MetaMasterClientServiceHandler(MetaMaster metaMaster) {
    mMetaMaster = metaMaster;
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.META_MASTER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public GetConfigurationTResponse getConfiguration(GetConfigurationTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcUtils.RpcCallable<GetConfigurationTResponse>) ()
        -> (new GetConfigurationTResponse(mMetaMaster.getConfiguration()
        .stream()
        .map(configProperty -> (configProperty.toThrift()))
        .collect(Collectors.toList()))));
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
            info.setMasterAddress(mMetaMaster.getRpcAddress().toString());
            break;
          case RPC_PORT:
            info.setRpcPort(mMetaMaster.getRpcAddress().getPort());
            break;
          case SAFE_MODE:
            info.setSafeMode(mMetaMaster.isInSafeMode());
            break;
          case START_TIME_MS:
            info.setStartTimeMs(mMetaMaster.getStartTimeMs());
            break;
          case UP_TIME_MS:
            info.setUpTimeMs(mMetaMaster.getUptimeMs());
            break;
          case VERSION:
            info.setVersion(RuntimeConstants.VERSION);
            break;
          case WEB_PORT:
            info.setWebPort(mMetaMaster.getWebPort());
            break;
          case ZOOKEEPER_ADDRESSES:
            if (Configuration.containsKey(PropertyKey.ZOOKEEPER_ADDRESS)) {
              info.setZookeeperAddresses(Arrays
                  .asList(Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS).split(",")));
            }
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
            MetricValue.forLong(entry.getValue().getCount()).toThrift());
      }

      for (Map.Entry<String, Gauge> entry : mr.getGauges().entrySet()) {
        Object value = entry.getValue().getValue();
        if (value instanceof Integer) {
          metricsMap.put(entry.getKey(),
              MetricValue.forLong(Long.valueOf((Integer) value)).toThrift());
        } else if (value instanceof Long) {
          metricsMap.put(entry.getKey(),
              MetricValue.forLong((Long) value).toThrift());
        } else if (value instanceof Double) {
          metricsMap.put(entry.getKey(),
              MetricValue.forDouble((Double) value).toThrift());
        }
      }
      return new GetMetricsTResponse(metricsMap);
    });
  }
}
