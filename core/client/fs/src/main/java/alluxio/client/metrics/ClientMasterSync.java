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
import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.ClientMetrics;
import alluxio.metrics.Metric;
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
  private static final Logger LOG =
      new SamplingLogger(LoggerFactory.getLogger(ClientMasterSync.class), 30 * Constants.SECOND_MS);

  /** Client for communicating to metrics master. */
  private final MetricsMasterClient mMasterClient;
  private final ConcurrentHashSet<String> mApplicationId;
  private final AlluxioConfiguration mConf;

  /**
   * Constructs a new {@link ClientMasterSync}.
   *
   * @param masterClient the master client
   * @param conf Alluxio configuration
   */
  public ClientMasterSync(MetricsMasterClient masterClient, AlluxioConfiguration conf) {
    mMasterClient = Preconditions.checkNotNull(masterClient, "masterClient");
    mApplicationId = new ConcurrentHashSet<>();
    mConf = conf;
  }

  /**
   * Adds a new application Id to be included in the metrics heartbeat.
   *
   * @param appId the application identifier to be added
   */
  public void addAppId(String appId) {
    Preconditions.checkNotNull(appId);
    Preconditions.checkArgument(!appId.equals(""));
    mApplicationId.add(appId);
  }

  /**
   * Removes and application Id from being included in the metrics heartbeat.
   *
   * @param appId the application identifier to be removed
   */
  public void removeAppId(String appId) {
    mApplicationId.remove(appId);
  }

  /**
   * Sends metrics to the master for each application Id that has been added.
   */
  public synchronized void heartbeat() {
    // Only perform the heartbeat if there are applications to track
    if (mApplicationId.size() > 0) {
      List<alluxio.grpc.ClientMetrics> clientMetrics = new ArrayList<>();
      List<Metric> allClientMetrics = MetricsSystem.allClientMetrics();
      String hostname = NetworkAddressUtils.getClientHostName(mConf);
      for (String appId : mApplicationId) {
        List<alluxio.grpc.Metric> metrics = new ArrayList<>();
        for (Metric metric : allClientMetrics) {
          metric.setInstanceId(appId);
          metrics.add(metric.toProto());
        }
        clientMetrics.add(ClientMetrics.newBuilder()
            .setHostname(hostname)
            .setClientId(appId)
            .addAllMetrics(metrics)
            .build());
      }
      try {
        mMasterClient.heartbeat(clientMetrics);
      } catch (IOException e) {
        // WARN instead of ERROR as metrics are not critical to the application function
        LOG.warn("Failed to send metrics to master: ", e);
        mMasterClient.disconnect();
      }
    }
  }
}
