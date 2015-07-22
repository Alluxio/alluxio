/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.worker.WorkerSource;

/**
 * BlockUserMetrics parses the user metrics and store them in a WorkerSource
 */
public class BlockUserMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Update user metrics from the heartbeat from a client.
   *
   * @param workerSource WorkerSource for collecting worker metrics
   * @param metrics The set of metrics the client has gathered since the last heartbeat
   */
  public void updateMetrics(WorkerSource workerSource, List<Long> metrics) {
    if (null != metrics && !metrics.isEmpty() && metrics.get(Constants.CLIENT_METRICS_VERSION_INDEX)
        == Constants.CLIENT_METRICS_VERSION) {
      workerSource.incBlocksReadLocal(metrics.get(Constants.BLOCKS_READ_LOCAL_INDEX));
      workerSource.incBlocksReadRemote(metrics.get(Constants.BLOCKS_READ_REMOTE_INDEX));
      workerSource.incBlocksWrittenLocal(metrics.get(Constants.BLOCKS_WRITTEN_LOCAL_INDEX));
      workerSource.incBlocksWrittenRemote(metrics.get(Constants.BLOCKS_WRITTEN_REMOTE_INDEX));
      workerSource.incBytesReadLocal(metrics.get(Constants.BYTES_READ_LOCAL_INDEX));
      workerSource.incBytesReadRemote(metrics.get(Constants.BYTES_READ_REMOTE_INDEX));
      workerSource.incBytesReadUfs(metrics.get(Constants.BYTES_READ_UFS_INDEX));
      workerSource.incBytesWrittenLocal(metrics.get(Constants.BYTES_WRITTEN_LOCAL_INDEX));
      workerSource.incBytesWrittenRemote(metrics.get(Constants.BYTES_WRITTEN_REMOTE_INDEX));
      workerSource.incBytesWrittenUfs(metrics.get(Constants.BYTES_WRITTEN_UFS_INDEX));
    }
  }
}