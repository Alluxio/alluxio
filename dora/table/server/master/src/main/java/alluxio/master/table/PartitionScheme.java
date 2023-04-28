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

package alluxio.master.table;

import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Layout;

import java.util.List;

/**
 * Interface of the partition scheme.
 */
public interface PartitionScheme {
  /**
   * Add a list of partitions.
   * @param partitions partitions to add
   */
  void addPartitions(List<Partition> partitions);

  /**
   * Get a list of partitions.
   *
   * @return a list of partitions
   */
  List<Partition> getPartitions();

  /**
   * @param spec the partition spec
   * @return the corresponding partition, or null if spec does not exist
   */
  Partition getPartition(String spec);

  /**
   * Get table layout.
   *
   * @return table info
   */
  Layout getTableLayout();

  /**
   * Get partition columns.
   *
   * @return partition columns
   */
  List<FieldSchema> getPartitionCols();

  /**
   * create a partition scheme object.
   *
   * @param partitions partitions
   * @param layout table layout
   * @param partCols table partition columns
   * @return a partition scheme object
   */
  static PartitionScheme create(List<Partition> partitions, Layout layout,
      List<FieldSchema> partCols) {
    if (partCols.isEmpty()) {
      return new UnpartitionedTableScheme(partitions);
    } else {
      return new PartitionedTableScheme(partitions, layout, partCols);
    }
  }
}
