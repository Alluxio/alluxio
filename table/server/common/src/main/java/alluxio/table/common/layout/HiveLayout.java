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

package alluxio.table.common.layout;

import alluxio.AlluxioURI;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.PartitionInfo;
import alluxio.table.common.Layout;
import alluxio.table.common.LayoutFactory;
import alluxio.table.common.transform.TransformContext;
import alluxio.table.common.transform.TransformDefinition;
import alluxio.table.common.transform.TransformPlan;

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hive layout implementation.
 */
public class HiveLayout implements Layout {
  private static final Logger LOG = LoggerFactory.getLogger(HiveLayout.class);

  public static final String TYPE = "hive";

  /**
   * Factory to create layout implementation.
   */
  public static class HiveLayoutFactory implements LayoutFactory {
    @Override
    public String getType() {
      return TYPE;
    }

    @Override
    public Layout create(alluxio.grpc.table.Layout layoutProto) {
      if (!TYPE.equals(layoutProto.getLayoutType())) {
        throw new IllegalStateException(
            "Cannot parse HiveLayout from layout type: " + layoutProto.getLayoutType());
      }
      if (!layoutProto.hasLayoutData()) {
        throw new IllegalStateException("Cannot parse layout from empty layout data");
      }

      try {
        PartitionInfo partitionInfo = PartitionInfo.parseFrom(layoutProto.getLayoutData());
        return new HiveLayout(partitionInfo, new ArrayList<>(layoutProto.getStatsMap().values()));
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException("Cannot parse HiveLayout from proto layout", e);
      }
    }
  }

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
    return TYPE;
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
  public Map<String, ColumnStatisticsInfo> getColumnStatsData() {
    return mPartitionStatsInfo;
  }

  @Override
  public TransformPlan getTransformPlan(TransformContext transformContext,
      TransformDefinition definition)
      throws IOException {
    // TODO(cc): check to see if output is also hive
    // TODO(cc): construct job definition to a new output location
    AlluxioURI outputPath = transformContext.getTransformPath();
    // TODO(cc): construct resulting layout
    return new TransformPlan(definition);
  }
}
