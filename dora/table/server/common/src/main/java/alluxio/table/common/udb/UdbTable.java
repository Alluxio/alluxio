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

package alluxio.table.common.udb;

import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.Schema;
import alluxio.table.common.UdbPartition;

import java.util.List;
import java.util.Map;

/**
 * The interface for the underdb table.
 */
public interface UdbTable {

  /**
   * @return the table name
   */
  String getName();

  /**
   * @return the table schema
   */
  Schema getSchema();

  /**
   * @return the table owner
   */
  String getOwner();

  /**
   * @return the map of parameters
   */
  Map<String, String> getParameters();

  /**
   * @return the list of partition columns
   */
  List<FieldSchema> getPartitionCols();

  /**
   * @return the layout for the table (could differ from partition layouts)
   */
  Layout getLayout();

  // TODO(gpang): generalize statistics
  /**
   * @return statistics of the table
   */
  List<ColumnStatisticsInfo> getStatistics();

  /**
   * @return returns partitions for the table
   */
  List<UdbPartition> getPartitions();
}
