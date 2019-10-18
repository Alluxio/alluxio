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

package alluxio.table.common;

import alluxio.AlluxioURI;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.LayoutSpec;
import alluxio.table.common.transform.TransformContext;
import alluxio.table.common.transform.TransformDefinition;
import alluxio.table.common.transform.TransformPlan;

import com.google.protobuf.Message;

import java.io.IOException;
import java.util.Map;

/**
 * An interface for a table/partition layout.
 */
public interface Layout {
  /**
   * @return the type of table/partition layout
   */
  String getType();

  /**
   * @return the layout specification
   */
  String getSpec();

  /**
   * @return a proto representing the data for this table/partition layout
   */
  Message getData();

  /**
   * @return a map of proto representing the statistics data for this partition
   */
  Map<String, ColumnStatisticsInfo> getColumnStatsData();

  /**
   * @return the location of the layout
   */
  AlluxioURI getLocation();

  /**
   * @param transformContext the {@link TransformContext}
   * @param definition the transform definition
   * @return a new {@code TransformPlan} representing the layout transformation
   */
  TransformPlan getTransformPlan(TransformContext transformContext, TransformDefinition definition)
      throws IOException;

  /**
   * @return the proto representation
   */
  default alluxio.grpc.table.Layout toProto() {
    return alluxio.grpc.table.Layout.newBuilder()
        .setLayoutType(getType())
        .setLayoutSpec(LayoutSpec.newBuilder()
            .setSpec(getSpec())
            .build())
        .setLayoutData(getData().toByteString())
        .putAllStats(getColumnStatsData())
        .build();
  }
}
