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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

/**
 * Unpartitoned table scheme.
 */
public class UnpartitionedTableScheme implements PartitionScheme {
  private final Partition mPartition;
  private final List<Partition> mPartitionList;

  /**
   * Constructor for UnpartitionedTableScheme.
   *
   * @param partitions a list of partitions
   */
  public UnpartitionedTableScheme(List<Partition> partitions) {
    Preconditions.checkArgument(partitions.size() == 1);
    mPartition = partitions.get(0);
    mPartitionList = ImmutableList.of(mPartition);
  }

  @Override
  public List<Partition> getPartitions() {
    return mPartitionList;
  }

  @Override
  public Layout getTableLayout() {
    return mPartition.getLayout().toProto();
  }

  @Override
  public List<FieldSchema> getPartitionCols() {
    return Collections.emptyList();
  }
}
