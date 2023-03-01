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

package alluxio.client.block.util;

import alluxio.ClientContext;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.client.block.AllMastersWorkerInfo;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import alluxio.retry.TimeoutRetry;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The util class for getting the worker info.
 */
public class WorkerInfoUtil {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerInfoUtil.class);
  private static final int RETRY_TIMEOUT = 5000;
  private static final int RETRY_INTERVAL = 500;

  /**
   * Get worker reports from all masters, including standby masters.
   * Can only be called when worker all master registration feature is enabled.
   *
   * @param configuration the cluster configuration
   * @param primaryMasterClient the block master client connecting to the primary
   * @param options the options to make the GetWorkerReport rpc
   * @return the aggregated worker info
   */
  public static AllMastersWorkerInfo getWorkerReportsFromAllMasters(
      AlluxioConfiguration configuration,
      BlockMasterClient primaryMasterClient,
      GetWorkerReportOptions options) throws IOException {
    Preconditions.checkState(
        configuration.getBoolean(PropertyKey.WORKER_REGISTER_TO_ALL_MASTERS),
        "GetWorkerReportsFromAllMasters is used to collect worker info from "
            + "all masters, including standby masters. "
            + "This method requires worker all master registration to be enabled.");

    Preconditions.checkState(
        options.getFieldRange().contains(GetWorkerReportOptions.WorkerInfoField.ID));
    Preconditions.checkState(
        options.getFieldRange().contains(GetWorkerReportOptions.WorkerInfoField.STATE));
    Preconditions.checkState(
        options.getFieldRange().contains(GetWorkerReportOptions.WorkerInfoField.ADDRESS));

    ClientContext clientContext = ClientContext.create(Configuration.global());
    MasterClientContext masterContext = MasterClientContext.newBuilder(clientContext).build();

    Preconditions.checkState(
        primaryMasterClient.getRemoteSockAddress() instanceof InetSocketAddress);
    InetSocketAddress primaryMasterAddress =
        (InetSocketAddress) primaryMasterClient.getRemoteSockAddress();
    List<InetSocketAddress> masterAddresses =
        ConfigurationUtils.getMasterRpcAddresses(configuration);
    Preconditions.checkState(masterAddresses.contains(primaryMasterAddress));

    Map<InetSocketAddress, List<WorkerInfo>> masterAddressToWorkerInfoMap = new HashMap<>();
    for (InetSocketAddress masterAddress : masterAddresses) {
      try (BlockMasterClient client = new RetryHandlingBlockMasterClient(
          masterContext, masterAddress, () -> new TimeoutRetry(RETRY_TIMEOUT, RETRY_INTERVAL))) {
        List<WorkerInfo> workerInfos = client.getWorkerReport(options);
        masterAddressToWorkerInfoMap.put(masterAddress, workerInfos);
      } catch (Exception e) {
        if (masterAddress.equals(primaryMasterAddress)) {
          LOG.error("Failed to get worker report from master: {}", masterContext, e);
          throw e;
        }
        LOG.warn("Failed to get worker report from master: {}", masterContext, e);
      }
    }
    return populateAllMastersWorkerInfo(primaryMasterAddress, masterAddressToWorkerInfoMap);
  }

  @VisibleForTesting
  @SuppressFBWarnings("WMI_WRONG_MAP_ITERATOR")
  static AllMastersWorkerInfo populateAllMastersWorkerInfo(
      InetSocketAddress primaryMasterAddress,
      Map<InetSocketAddress, List<WorkerInfo>> masterAddressToWorkerInfoMap) {
    Map<Long, List<Pair<InetSocketAddress, WorkerInfo>>> workerIdInfoMap = new HashMap<>();
    Map<Long, InetSocketAddress> workerIdAddressMap = new HashMap<>();
    List<WorkerInfo> workerInfosFromPrimaryMaster = null;

    for (InetSocketAddress masterAddress : masterAddressToWorkerInfoMap.keySet()) {
      List<WorkerInfo> workerInfo = masterAddressToWorkerInfoMap.get(masterAddress);
      if (masterAddress.equals(primaryMasterAddress)) {
        workerInfosFromPrimaryMaster = workerInfo;
      }
      for (WorkerInfo info : workerInfo) {
        workerIdInfoMap.compute(info.getId(), (k, v) -> {
          if (v == null) {
            v = new ArrayList<>();
          }
          v.add(new Pair<>(masterAddress, info));
          return v;
        });
        workerIdAddressMap.compute(info.getId(), (k, v) -> {
          InetSocketAddress workerAddress =
              InetSocketAddress.createUnresolved(info.getAddress().getHost(),
                  info.getAddress().getRpcPort());
          if (v == null) {
            return workerAddress;
          }
          if (!v.equals(workerAddress)) {
            throw new RuntimeException(String.format(
                "The same worker id %d corresponds to multiple worker name %s %s",
                k, v, workerAddress));
          }
          return v;
        });
      }
    }
    return new AllMastersWorkerInfo(workerIdAddressMap,
        new ArrayList<>(masterAddressToWorkerInfoMap.keySet()),
        primaryMasterAddress,
        workerInfosFromPrimaryMaster, workerIdInfoMap);
  }
}
