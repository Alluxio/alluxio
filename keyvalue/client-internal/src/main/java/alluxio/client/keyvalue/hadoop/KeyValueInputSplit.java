/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client.keyvalue.hadoop;

import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.AlluxioException;
import alluxio.thrift.PartitionInfo;

import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implements {@link InputSplit}, each split contains one partition of the {@link KeyValueSystem}.
 */
@NotThreadSafe
final class KeyValueInputSplit extends InputSplit {
  private static final long INVALID_BLOCK_ID = -1;

  /** The block store client. */
  private AlluxioBlockStore mBlockStore;
  // TODO(cc): Use the concept of partition ID in the future.
  /** The ID of the block represented by this split. */
  private long mBlockId;

  /**
   * Default constructor, to be used together with {@link #readFields(DataInput)} when
   * de-serializing {@link KeyValueInputSplit}.
   */
  public KeyValueInputSplit() {
    mBlockStore = AlluxioBlockStore.get();
    mBlockId = INVALID_BLOCK_ID;
  }

  /**
   * Creates an {@link InputSplit} for a partition.
   *
   * @param partitionInfo the partition info
   */
  public KeyValueInputSplit(PartitionInfo partitionInfo) {
    mBlockStore = AlluxioBlockStore.get();
    mBlockId = partitionInfo.getBlockId();
  }

  @Override
  public long getLength() throws IOException {
    return mBlockStore.getInfo(mBlockId).getLength();
  }

  @Override
  public String[] getLocations() throws IOException {
    try {
      List<BlockWorkerInfo> workersInfo = mBlockStore.getWorkerInfoList();
      int workersInfoSize = workersInfo.size();
      String[] locations = new String[workersInfoSize];
      for (int i = 0; i < workersInfoSize; i++) {
        locations[i] = workersInfo.get(i).getNetAddress().getHost();
      }
      return locations;
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(mBlockId);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    mBlockStore = AlluxioBlockStore.get();
    mBlockId = dataInput.readLong();
  }

  /**
   * @return id of the partition, currently is the block ID of the partition
   */
  public long getPartitionId() {
    return mBlockId;
  }
}
