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

package tachyon.worker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import tachyon.Constants;

/**
 * ClientMetrics is used to pass client metrics from client to worker by user heartbeat.
 */
public class ClientMetrics {
  private List<Long> mMetrics;

  public ClientMetrics() {
    mMetrics = createDefaultMetrics();
  }

  private List<Long> createDefaultMetrics() {
    List<Long> defaultMetrics =
        new ArrayList<Long>(Collections.nCopies(Constants.CLIENT_METRICS_SIZE, 0L));
    defaultMetrics.set(Constants.CLIENT_METRICS_VERSION_INDEX, Constants.CLIENT_METRICS_VERSION);
    return defaultMetrics;
  }

  public synchronized List<Long> getHeartbeatData() {
    List<Long> ret = mMetrics;
    mMetrics = createDefaultMetrics();
    return ret;
  }

  public synchronized void incBlocksReadLocal(long n) {
    mMetrics.set(Constants.BLOCKS_READ_LOCAL_INDEX,
        mMetrics.get(Constants.BLOCKS_READ_LOCAL_INDEX) + n);
  }

  public synchronized void incBlocksReadRemote(long n) {
    mMetrics.set(Constants.BLOCKS_READ_REMOTE_INDEX,
        mMetrics.get(Constants.BLOCKS_READ_REMOTE_INDEX) + n);
  }

  public synchronized void incBlocksWrittenLocal(long n) {
    mMetrics.set(Constants.BLOCKS_WRITTEN_LOCAL_INDEX,
        mMetrics.get(Constants.BLOCKS_WRITTEN_LOCAL_INDEX) + n);
  }

  public synchronized void incBlocksWrittenRemote(long n) {
    mMetrics.set(Constants.BLOCKS_WRITTEN_REMOTE_INDEX,
        mMetrics.get(Constants.BLOCKS_WRITTEN_REMOTE_INDEX) + n);
  }

  public synchronized void incBytesReadLocal(long n) {
    mMetrics.set(Constants.BYTES_READ_LOCAL_INDEX,
        mMetrics.get(Constants.BYTES_READ_LOCAL_INDEX) + n);
  }

  public synchronized void incBytesReadRemote(long n) {
    mMetrics.set(Constants.BYTES_READ_REMOTE_INDEX,
        mMetrics.get(Constants.BYTES_READ_REMOTE_INDEX) + n);
  }

  public synchronized void incBytesReadUfs(long n) {
    mMetrics.set(Constants.BYTES_READ_UFS_INDEX, mMetrics.get(Constants.BYTES_READ_UFS_INDEX) + n);
  }

  public synchronized void incBytesWrittenLocal(long n) {
    mMetrics.set(Constants.BYTES_WRITTEN_LOCAL_INDEX,
        mMetrics.get(Constants.BYTES_WRITTEN_LOCAL_INDEX) + n);
  }

  public synchronized void incBytesWrittenRemote(long n) {
    mMetrics.set(Constants.BYTES_WRITTEN_REMOTE_INDEX,
        mMetrics.get(Constants.BYTES_WRITTEN_REMOTE_INDEX) + n);
  }

  public synchronized void incBytesWrittenUfs(long n) {
    mMetrics.set(Constants.BYTES_WRITTEN_UFS_INDEX,
        mMetrics.get(Constants.BYTES_WRITTEN_UFS_INDEX) + n);
  }
}
