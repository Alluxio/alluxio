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
import alluxio.grpc.catalog.FieldSchema;
import alluxio.grpc.catalog.PartitionInfo;
import alluxio.grpc.catalog.TableViewInfo;

import java.util.List;

/**
 * A view or representation of a table.
 */
public interface TableView {
  /**
   * @return the base location
   */
  String getBaseLocation();

  /**
   * @return the statistics
   */
  List<ColumnStatisticsInfo> getStatistics();

  /**
   * @return the partition information
   */
  List<PartitionInfo> getPartitions();

  /**
   * @return the partition keys
   */
  List<FieldSchema> getPartitionCols();

  /**
   * @param viewName the name of the view
   * @return the proto representation
   */
  TableViewInfo toProto(String viewName);
}
