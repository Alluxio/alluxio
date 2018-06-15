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

package alluxio.client;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.master.MasterClientConfig;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.BackupTOptions;
import alluxio.thrift.GetConfigReportTOptions;
import alluxio.thrift.GetConfigurationTOptions;
import alluxio.thrift.GetMasterInfoTOptions;
import alluxio.thrift.GetMetricsTOptions;
import alluxio.thrift.MetaMasterClientService;
import alluxio.wire.BackupResponse;
import alluxio.wire.ConfigCheckReport;
import alluxio.wire.ConfigProperty;
import alluxio.wire.MasterInfo;
import alluxio.wire.MasterInfo.MasterInfoField;
import alluxio.wire.MetricValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the meta master.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety and
 * support for retries.
 */
@ThreadSafe
public final class RetryHandlingMetaMasterClient extends AbstractMasterClient
    implements MetaMasterClient {
  private MetaMasterClientService.Client mClient;

  /**
   * Creates a new meta master client.
   *
   * @param conf master client configuration
   */
  public RetryHandlingMetaMasterClient(MasterClientConfig conf) {
    super(conf);
    mClient = null;
  }

  @Override
  protected AlluxioService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.META_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.META_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = new MetaMasterClientService.Client(mProtocol);
  }

  @Override
  public synchronized BackupResponse backup(String targetDirectory,
                                            boolean localFileSystem) throws IOException {
    return retryRPC(
        () -> BackupResponse.fromThrift(mClient.backup(new BackupTOptions()
            .setTargetDirectory(targetDirectory).setLocalFileSystem(localFileSystem))));
  }

  @Override
  public synchronized ConfigCheckReport getConfigReport() throws IOException {
    return retryRPC(() -> ConfigCheckReport.fromThrift(mClient
        .getConfigReport(new GetConfigReportTOptions()).getReport()));
  }

  @Override
  public synchronized List<ConfigProperty> getConfiguration() throws IOException {
    return retryRPC(() -> mClient.getConfiguration(new GetConfigurationTOptions())
          .getConfigList().stream()
          .map(ConfigProperty::fromThrift)
          .collect(Collectors.toList()));
  }

  @Override
  public synchronized MasterInfo getMasterInfo(final Set<MasterInfoField> fields)
      throws IOException {
    return retryRPC(() -> {
      Set<alluxio.thrift.MasterInfoField> thriftFields = new HashSet<>();
      if (fields == null) {
        thriftFields = null;
      } else {
        for (MasterInfo.MasterInfoField field : fields) {
          thriftFields.add(field.toThrift());
        }
      }
      return MasterInfo.fromThrift(
          mClient.getMasterInfo(new GetMasterInfoTOptions(thriftFields)).getMasterInfo());
    });
  }

  @Override
  public synchronized Map<String, MetricValue> getMetrics() throws AlluxioStatusException {
    return retryRPC(() -> {
      Map<String, MetricValue> wireMap = new HashMap<>();
      for (Map.Entry<String, alluxio.thrift.MetricValue> entry :
          mClient.getMetrics(new GetMetricsTOptions()).getMetricsMap().entrySet()) {
        wireMap.put(entry.getKey(), MetricValue.fromThrift(entry.getValue()));
      }
      return wireMap;
    });
  }
}
