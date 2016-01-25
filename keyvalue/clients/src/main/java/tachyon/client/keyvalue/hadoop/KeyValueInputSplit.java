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

package tachyon.client.keyvalue.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.mapred.InputSplit;

import tachyon.client.block.BlockWorkerInfo;
import tachyon.client.block.TachyonBlockStore;
import tachyon.client.keyvalue.KeyValueStores;
import tachyon.exception.TachyonException;
import tachyon.thrift.PartitionInfo;

/**
 * Implements {@link InputSplit}, each split contains one partition of the {@link KeyValueStores}.
 */
@NotThreadSafe
final class KeyValueInputSplit implements InputSplit {
  private static final long INVALID_BLOCK_ID = -1;

  /** The block store client */
  private TachyonBlockStore mBlockStore;
  // TODO(cc): Use the concept of partition ID in the future.
  /** The ID of the block represented by this split */
  private long mBlockId;

  /**
   * Default constructor, to be used in de-serialization of {@link KeyValueInputSplit}.
   */
  public KeyValueInputSplit() {
    mBlockStore = TachyonBlockStore.get();
    mBlockId = INVALID_BLOCK_ID;
  }

  /**
   * Creates an {@link InputSplit} for a partition.
   *
   * @param partitionInfo the partition info
   */
  public KeyValueInputSplit(PartitionInfo partitionInfo) {
    mBlockStore = TachyonBlockStore.get();
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
      for (int i = 0; i < workersInfoSize; i ++) {
        locations[i] = workersInfo.get(i).getNetAddress().getHost();
      }
      return locations;
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(mBlockId);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    mBlockStore = TachyonBlockStore.get();
    mBlockId = dataInput.readLong();
  }

  /**
   * @return id of the partition, currently is the block ID of the partition
   */
  public long getPartitionId() {
    return mBlockId;
  }
}
