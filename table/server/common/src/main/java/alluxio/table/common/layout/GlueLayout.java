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
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.job.plan.transform.HiveConstants;
import alluxio.table.common.Layout;
import alluxio.table.common.transform.TransformContext;
import alluxio.table.common.transform.TransformDefinition;
import alluxio.table.common.transform.TransformPlan;
import alluxio.util.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Glue table layout implementation.
 * Glue use same proto as hive.
 */
public class GlueLayout implements Layout {
  private static final Logger LOG = LoggerFactory.getLogger(GlueLayout.class);

  private final PartitionInfo mPartitionInfo;
  private final Map<String, ColumnStatisticsInfo> mPartitionStatsInfo;

  /**
   * Glue layout init.
   *
   * @param partitionInfo partition information
   * @param stats column statistics
   */
  public GlueLayout(PartitionInfo partitionInfo, List<ColumnStatisticsInfo> stats) {
    mPartitionInfo = partitionInfo;
    mPartitionStatsInfo = stats.stream().collect(Collectors.toMap(
       ColumnStatisticsInfo::getColName, e -> e, (e1, e2) -> e2));
  }

  @Override
  public String getType() {
    return GlueLayoutFactory.TYPE;
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
  public AlluxioURI getLocation() {
    return new AlluxioURI(mPartitionInfo.getStorage().getLocation());
  }

  @Override
  public Map<String, ColumnStatisticsInfo> getColumnStatsData() {
    return mPartitionStatsInfo;
  }

  @Override
  public TransformPlan getTransformPlan(
      TransformContext transformContext, TransformDefinition definition)
      throws IOException {
    AlluxioURI outputPath = transformContext.generateTransformedPath();
    AlluxioURI outputUri = new AlluxioURI(
        ConfigurationUtils.getSchemeAuthority(ServerConfiguration.global())
            + outputPath.getPath());
    GlueLayout transformedLayout = transformLayout(outputUri);
    return new TransformPlan(this, transformedLayout, definition);
  }

  private GlueLayout transformLayout(AlluxioURI transformedUri) {
    // TODO(cc): assumption here is the transformed data is in Parquet format.
    PartitionInfo info = mPartitionInfo.toBuilder()
        .putAllParameters(mPartitionInfo.getParametersMap())
        .setStorage(mPartitionInfo.getStorage().toBuilder()
            .setStorageFormat(mPartitionInfo.getStorage().getStorageFormat().toBuilder()
                .setSerde(HiveConstants.PARQUET_SERDE_CLASS)
                .setInputFormat(HiveConstants.PARQUET_INPUT_FORMAT_CLASS)
                .setOutputFormat(HiveConstants.PARQUET_OUTPUT_FORMAT_CLASS)
                .build())
            .setLocation(transformedUri.toString())
            .build())
        .build();
    List<ColumnStatisticsInfo> stats = new ArrayList<>(mPartitionStatsInfo.values());
    return new GlueLayout(info, stats);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GlueLayout)) {
      return false;
    }
    GlueLayout that = (GlueLayout) obj;
    return Objects.equals(mPartitionInfo, that.mPartitionInfo)
        && Objects.equals(mPartitionStatsInfo, that.mPartitionStatsInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mPartitionInfo, mPartitionStatsInfo);
  }
}
