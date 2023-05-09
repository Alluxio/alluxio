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

package alluxio.master.file.mdsync;

import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;

/**
 * Sync operation metrics.
 */
public class SyncOperationMetrics {
  public static final Counter CREATE_COUNT
      = MetricsSystem.counter(MetricKey.MASTER_METADATA_SYNC_FILES_CREATED.getName());
  public static final Counter DELETE_COUNT
      = MetricsSystem.counter(MetricKey.MASTER_METADATA_SYNC_FILES_DELETED.getName());
  public static final Counter RECREATED_COUNT
      = MetricsSystem.counter(MetricKey.MASTER_METADATA_SYNC_FILES_RECREATED.getName());
  public static final Counter UPDATE_COUNT
      = MetricsSystem.counter(MetricKey.MASTER_METADATA_SYNC_FILES_UPDATED.getName());
  public static final Counter SKIP_CONCURRENT_UPDATE_COUNT
      = MetricsSystem.counter(
          MetricKey.MASTER_METADATA_SYNC_FILES_SKIPPED_CONCURRENT_UPDATE.getName());
  public static final Counter SKIP_MOUNT_POINT_COUNT
      = MetricsSystem.counter(MetricKey.MASTER_METADATA_SYNC_FILES_SKIPPED_MOUNT_POINT.getName());
  public static final Counter NOOP_COUNT
      = MetricsSystem.counter(MetricKey.MASTER_METADATA_SYNC_FILES_NOOP.getName());
  public static final Counter SKIPPED_NON_PERSISTED_COUNT
      = MetricsSystem.counter(MetricKey.MASTER_METADATA_SYNC_FILES_SKIPPED_NON_PERSISTED.getName());
}
