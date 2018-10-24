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

import alluxio.client.file.FileSystemContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;

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
public final class ClientMasterSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ClientMasterSync.class);

  /** Client for communicating to metrics master. */
  private final MetricsMasterClient mMasterClient;
  private final FileSystemContext mContext;

  /**
   * Constructs a new {@link ClientMasterSync}.
   *
   * @param masterClient the master client
   * @param context the filesystem context
   */
  public ClientMasterSync(MetricsMasterClient masterClient, FileSystemContext context) {
    mMasterClient = Preconditions.checkNotNull(masterClient, "masterClient");
    mContext = Preconditions.checkNotNull(context, "context");
  }

  @Override
  public synchronized void heartbeat() throws InterruptedException {
    List<alluxio.thrift.Metric> metrics = new ArrayList<>();
    for (Metric metric : MetricsSystem.allClientMetrics()) {
      metric.setInstanceId(mContext.getId());
      metrics.add(metric.toThrift());
    }
    try {
      mMasterClient.heartbeat(metrics);
    } catch (IOException e) {
      // An error occurred, log and ignore it or error if heartbeat timeout is reached.
      LOG.error("Failed to heartbeat to the metrics master:", e);
      mMasterClient.disconnect();
    }
  }

  @Override
  public void close() {
  }
}
