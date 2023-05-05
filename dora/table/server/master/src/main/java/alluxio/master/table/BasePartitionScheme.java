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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base implementation of PartitionScheme with default implementation of
 * {@link PartitionScheme#getPartition(String)} and {@link PartitionScheme#getPartitions()}.
 */
public abstract class BasePartitionScheme implements PartitionScheme {
  protected final List<Partition> mPartitions;
  protected final Map<String, Partition> mSpecToPartition;

  /**
   * A map from partition spec to partitions is computed from the partitions.
   *
   * @param partitions list of partitions
   */
  public BasePartitionScheme(List<Partition> partitions) {
    mPartitions = partitions;
    mSpecToPartition = new HashMap<>();
    for (Partition partition : mPartitions) {
      mSpecToPartition.put(partition.getSpec(), partition);
    }
  }

  @Override
  public void addPartitions(List<Partition> partitions) {
    mPartitions.addAll(partitions);
    for (Partition partition : mPartitions) {
      mSpecToPartition.put(partition.getSpec(), partition);
    }
  }

  @Override
  public List<Partition> getPartitions() {
    return Collections.unmodifiableList(mPartitions);
  }

  @Override
  public Partition getPartition(String spec) {
    return mSpecToPartition.get(spec);
  }
}
