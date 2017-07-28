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

package alluxio.hadoop.mapreduce;

import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.thrift.PartitionInfo;

import org.apache.hadoop.io.Writable;
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
final class KeyValueInputSplit extends InputSplit implements Writable {
  private static final long INVALID_BLOCK_ID = -1;

  /** The block store client. */
  private final AlluxioBlockStore mBlockStore;
  // TODO(cc): Use the concept of partition ID in the future.
  /** The ID of the block represented by this split. */
  private long mBlockId;

  /**
   * Default constructor, to be used together with {@link #readFields(DataInput)} when
   * de-serializing {@link KeyValueInputSplit}.
   */
  public KeyValueInputSplit() {
    mBlockStore = AlluxioBlockStore.create();
    mBlockId = INVALID_BLOCK_ID;
  }

  /**
   * Creates an {@link InputSplit} for a partition.
   *
   * @param partitionInfo the partition info
   */
  public KeyValueInputSplit(PartitionInfo partitionInfo) {
    mBlockStore = AlluxioBlockStore.create();
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
    } catch (AlluxioStatusException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(mBlockId);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    mBlockId = dataInput.readLong();
  }

  /**
   * @return id of the partition, currently is the block ID of the partition
   */
  public long getPartitionId() {
    return mBlockId;
  }
}
