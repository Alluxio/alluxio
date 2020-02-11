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

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.ClientMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.util.logging.SamplingLogger;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Task that carries the client metrics information to master through heartheat. This class manages
 * its own {@link MetricsMasterClient}.
 *
 * If the task fails to heartbeat to the master, it will destroy its old master client and recreate
 * it before retrying.
 */
@ThreadSafe
public final class ClientMasterSync {
  private static final Logger SAMPLING_LOG =
      new SamplingLogger(LoggerFactory.getLogger(ClientMasterSync.class), 30 * Constants.SECOND_MS);

  /**
   * Client for communicating to metrics master.
   */
  private final MetricsMasterClient mMasterClient;
  private final String mApplicationId;
  private final AlluxioConfiguration mConf;

  /**
   * Constructs a new {@link ClientMasterSync}.
   *
   * @param appId the application id to send with metrics
   * @param masterClient the master client
   * @param conf Alluxio configuration
   */
  public ClientMasterSync(String appId, MetricsMasterClient masterClient,
      AlluxioConfiguration conf) {
    mMasterClient = Preconditions.checkNotNull(masterClient, "masterClient");
    mApplicationId = Preconditions.checkNotNull(appId);
    mConf = conf;
  }

  /**
   * Sends metrics to the master keyed with appId and client hostname.
   */
  public synchronized void heartbeat() {
    // TODO(zac): Support per FileSystem instance metrics
    // Currently we only support JVM-level metrics. A list is used here because in the near
    // future we will support sending per filesystem client-level metrics.
    List<alluxio.grpc.ClientMetrics> fsClientMetrics = new ArrayList<>();
    String hostname = NetworkAddressUtils.getClientHostName(mConf);
    List<alluxio.grpc.Metric> metrics = MetricsSystem.reportClientMetrics();
    if (metrics.size() == 0) {
      // Likely when all should report metrics are counters
      // and this client doesn't do any actual operations (only used for meta sync)
      return;
    }
    fsClientMetrics.add(ClientMetrics.newBuilder()
        .setHostname(hostname)
        .setClientId(mApplicationId)
        .addAllMetrics(metrics)
        .build());
    try {
      mMasterClient.heartbeat(fsClientMetrics);
    } catch (IOException e) {
      // WARN instead of ERROR as metrics are not critical to the application function
      SAMPLING_LOG.warn("Failed to send metrics to master: {}", e.toString());
    }
  }
}
