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

import alluxio.grpc.catalog.ColumnStatisticsInfo;
import alluxio.grpc.catalog.LayoutSpec;

import com.google.protobuf.Message;

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
   * @return the proto representation
   */
  default alluxio.grpc.catalog.Layout toProto() {
    return alluxio.grpc.catalog.Layout.newBuilder()
        .setLayoutType(getType())
        .setLayoutSpec(LayoutSpec.newBuilder()
            .setSpec(getSpec())
            .build())
        .setLayoutData(getData().toByteString())
        .putAllStats(getColumnStatsData())
        .build();
  }
}
