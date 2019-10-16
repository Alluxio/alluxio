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

import alluxio.grpc.table.UdbTableInfo;

import java.util.List;

/**
 * Interface of the partition scheme.
 */
public interface PartitionScheme {
  /**
   * Get a list of partitions.
   *
   * @return a list of partitions
   */
  List<Partition> getPartitions();

  /**
   * Get table layout.
   *
   * @return table info
   */
  UdbTableInfo getTableLayout();

  /**
   * create a partition scheme object.
   *
   * @param partitions partitions
   * @param tableInfo table info
   * @param unpartitioned table is unpartitioned
   * @return a partition scheme object
   */
  static PartitionScheme createPartitionScheme(List<Partition> partitions, UdbTableInfo tableInfo,
      boolean unpartitioned) {
    if (unpartitioned) {
      return new UnpartitionedTableScheme(partitions, tableInfo);
    } else {
      return new PartitionedTableScheme(partitions, tableInfo);
    }
  }
}
