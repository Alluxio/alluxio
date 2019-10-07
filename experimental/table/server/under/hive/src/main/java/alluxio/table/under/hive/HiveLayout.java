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

import alluxio.grpc.catalog.ColumnStatisticsInfo;
import alluxio.grpc.catalog.PartitionInfo;
import alluxio.table.common.Layout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hive table implementation.
 */
public class HiveLayout implements Layout {
  private static final Logger LOG = LoggerFactory.getLogger(HiveLayout.class);

  private final PartitionInfo mPartitionInfo;
  private final Map<String, ColumnStatisticsInfo> mPartitionStatsInfo;

  /**
   * Creates an instance.
   *
   * @param partitionInfo the partition info
   * @param stats column statistics
   */
  public HiveLayout(PartitionInfo partitionInfo, List<ColumnStatisticsInfo> stats) {
    mPartitionInfo = partitionInfo;
    mPartitionStatsInfo = stats.stream().collect(Collectors.toMap(
        ColumnStatisticsInfo::getColName, e -> e, (e1, e2) -> e2));
  }

  @Override
  public String getType() {
    // TODO(gpang): make part of a layout registry
    return "hive";
  }

  @Override
  public String getSpec() {
    return mPartitionInfo.getPartitionName();
  }

  @Override
  public PartitionInfo getData() {
    return mPartitionInfo;
  }

  @Override
  public String getLocation() {
    return mPartitionInfo.getStorage().getLocation();
  }

  @Override
  public Layout transform(String type, String location) throws IOException {
    if (type.equals("hive")) {
      PartitionInfo info = mPartitionInfo.toBuilder()
          .setStorage(mPartitionInfo.getStorage().toBuilder()
              .setLocation(location)
              .build())
          .build();
      List<ColumnStatisticsInfo> stats = new ArrayList<>(mPartitionStatsInfo.values());
      return new HiveLayout(info, stats);
    } else {
      throw new IOException("Unknown type: " + type);
    }
  }

  @Override
  public Map<String, ColumnStatisticsInfo> getColumnStatsData() {
    return mPartitionStatsInfo;
  }
}
