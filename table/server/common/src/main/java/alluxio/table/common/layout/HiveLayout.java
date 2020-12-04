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
import alluxio.grpc.table.layout.hive.StorageFormat;
import alluxio.job.plan.transform.HiveConstants;
import alluxio.table.common.Layout;
import alluxio.table.common.LayoutFactory;
import alluxio.table.common.transform.TransformContext;
import alluxio.table.common.transform.TransformDefinition;
import alluxio.table.common.transform.TransformPlan;
import alluxio.util.ConfigurationUtils;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
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
  public AlluxioURI getLocation() {
    return new AlluxioURI(mPartitionInfo.getStorage().getLocation());
  }

  @Override
  public Map<String, ColumnStatisticsInfo> getColumnStatsData() {
    return mPartitionStatsInfo;
  }

  private HiveLayout transformLayout(AlluxioURI transformedUri, TransformDefinition definition) {
    final Properties properties = definition.getProperties();

    // TODO(cc): assumption here is the transformed data is in Parquet format.
    final StorageFormat.Builder storageFormatBuilder = mPartitionInfo.getStorage()
        .getStorageFormat().toBuilder()
        .setSerde(HiveConstants.PARQUET_SERDE_CLASS)
        .setInputFormat(HiveConstants.PARQUET_INPUT_FORMAT_CLASS)
        .setOutputFormat(HiveConstants.PARQUET_OUTPUT_FORMAT_CLASS);

    final String compressionKey = alluxio.job.plan.transform.PartitionInfo.PARQUET_COMPRESSION;
    final String compression = properties.getProperty(compressionKey);
    if (!StringUtils.isEmpty(compression)) {
      storageFormatBuilder.putSerdelibParameters(compressionKey, compression);
    }

    PartitionInfo info = mPartitionInfo.toBuilder()
        .putAllParameters(mPartitionInfo.getParametersMap())
        .setStorage(mPartitionInfo.getStorage().toBuilder()
            .setStorageFormat(storageFormatBuilder
                .build())
            .setLocation(transformedUri.toString())
            .build())
        .build();
    List<ColumnStatisticsInfo> stats = new ArrayList<>(mPartitionStatsInfo.values());
    return new HiveLayout(info, stats);
  }

  @Override
  public TransformPlan getTransformPlan(TransformContext transformContext,
      TransformDefinition definition) throws IOException {
    AlluxioURI outputPath = transformContext.generateTransformedPath();
    AlluxioURI outputUri = new AlluxioURI(
        ConfigurationUtils.getSchemeAuthority(ServerConfiguration.global())
        + outputPath.getPath());
    HiveLayout transformedLayout = transformLayout(outputUri, definition);
    return new TransformPlan(this, transformedLayout, definition);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof HiveLayout)) {
      return false;
    }
    HiveLayout that = (HiveLayout) obj;
    return Objects.equals(mPartitionInfo, that.mPartitionInfo)
        && Objects.equals(mPartitionStatsInfo, that.mPartitionStatsInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mPartitionInfo, mPartitionStatsInfo);
  }
}
