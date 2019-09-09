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

import alluxio.grpc.FieldSchema;
import alluxio.grpc.FileStatistics;
import alluxio.grpc.PartitionInfo;

import java.util.List;
import java.util.Map;

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
  Map<String, FileStatistics> getStatistics();

  /**
   * @return the partition information
   */
  List<PartitionInfo> getPartitions();

  /**
   * @return the partition keys
   */
  List<FieldSchema> getPartitionCols();
}
