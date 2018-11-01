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

package alluxio.underfs.hdfs;

import alluxio.AlluxioURI;
import alluxio.SyncInfo;

import java.util.List;

/**
 * The noop HDFS Active Sync provider.
 */
public class NoopHdfsActiveSyncProvider implements HdfsActiveSyncProvider {
  @Override
  public SyncInfo getActivitySyncInfo(List<AlluxioURI> syncPointList) {
    return SyncInfo.emptyInfo();
  }
}
