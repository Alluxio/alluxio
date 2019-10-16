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

import alluxio.grpc.table.Storage;
import alluxio.grpc.table.UdbTableInfo;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;

/**
 * Unpartitoned table scheme.
 */
public class UnpartitionedTableScheme implements PartitionScheme {
  private Partition mPartition;
  private UdbTableInfo mBaseInfo;

  /**
   * Constructor for UnpartitionedTableScheme.
   *
   * @param partitions a list of partitions
   * @param tableInfo table info
   */
  public UnpartitionedTableScheme(List<Partition> partitions, UdbTableInfo tableInfo) {
    Preconditions.checkArgument(partitions.size() == 1);
    mPartition = partitions.get(0);
    mBaseInfo = tableInfo;
  }

  @Override
  public List<Partition> getPartitions() {
    return Arrays.asList(mPartition);
  }

  @Override
  public UdbTableInfo getTableLayout() {
    if (mBaseInfo.hasHiveTableInfo()) {
      // check if the location is up-to-date
      if (mBaseInfo.getHiveTableInfo().getStorage().getLocation()
          .equals(mPartition.getLayout().getLocation())) {
        return mBaseInfo;
      }
      // update the location
      Storage storage = mBaseInfo.getHiveTableInfo().getStorage().toBuilder()
          .setLocation(mPartition.getLayout().getLocation()).build();
      mBaseInfo = mBaseInfo.toBuilder().setHiveTableInfo(mBaseInfo.getHiveTableInfo()
          .toBuilder().setStorage(storage).build()).build();
      return mBaseInfo;
    }
    throw new UnsupportedOperationException("Table layout not supported");
  }
}
