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

import java.util.Collections;
import java.util.List;

/**
 * Unpartitoned table scheme.
 */
public class UnpartitionedTableScheme extends BasePartitionScheme {
  private final Partition mPartition;

  /**
   * Constructor for UnpartitionedTableScheme.
   *
   * @param partitions a list of partitions
   */
  public UnpartitionedTableScheme(List<Partition> partitions) {
    super(partitions);
    Preconditions.checkArgument(partitions.size() == 1);
    mPartition = partitions.get(0);
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
