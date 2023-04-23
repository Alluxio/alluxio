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
 * Partitioned Table Scheme.
 */
public class PartitionedTableScheme extends BasePartitionScheme {
  private final Layout mTableInfo;
  private final List<FieldSchema> mPartCols;

  /**
   * constructor for PartitionedTableScheme.
   *
   * @param partitions list of partitions
   * @param tableInfo table info
   * @param partCols partition columns
   */
  PartitionedTableScheme(List<Partition> partitions, Layout tableInfo,
      List<FieldSchema> partCols) {
    super(partitions);
    mTableInfo = tableInfo;
    mPartCols = partCols;
  }

  @Override
  public Layout getTableLayout() {
    return mTableInfo;
  }

  @Override
  public List<FieldSchema> getPartitionCols() {
    return mPartCols;
  }
}
