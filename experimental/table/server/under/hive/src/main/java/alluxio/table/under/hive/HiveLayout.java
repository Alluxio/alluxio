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

package alluxio.table.under.hive;

import alluxio.grpc.catalog.PartitionInfo;
import alluxio.table.common.Layout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive table implementation.
 */
public class HiveLayout implements Layout {
  private static final Logger LOG = LoggerFactory.getLogger(HiveLayout.class);

  private final PartitionInfo mPartitionInfo;

  /**
   * Creates an instance.
   *
   * @param partitionInfo the partition info
   */
  public HiveLayout(PartitionInfo partitionInfo) {
    mPartitionInfo = partitionInfo;
  }

  @Override
  public String getType() {
    // TODO(gpang): make part of a layout registry
    return "hive";
  }

  @Override
  public String getSpec() {
    // TODO(gpang): implement
    return "";
  }

  @Override
  public PartitionInfo getData() {
    return mPartitionInfo;
  }
}
