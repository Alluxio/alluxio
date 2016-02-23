/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.keyvalue;

import alluxio.exception.AlluxioException;
import alluxio.thrift.PartitionInfo;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * {@link KeyValueIterator} to iterate over key-value pairs in {@link KeyValueSystem}.
 */
@NotThreadSafe
public final class KeyValueStoreIterator implements KeyValueIterator {
  /** All partitions. */
  private List<PartitionInfo> mPartitions;
  /** Index of the partition being visited. */
  private int mPartitionIndex = -1;
  /** Iterator of the partition being visited. */
  private KeyValueIterator mPartitionIterator;

  /**
   * @param partitions the partitions to use
   * @throws IOException if a non-Alluxio related exception occurs
   * @throws AlluxioException if a {@link KeyValuePartitionReader} cannot be created or iterated
   *         over
   */
  public KeyValueStoreIterator(List<PartitionInfo> partitions)
      throws IOException, AlluxioException {
    mPartitions = Preconditions.checkNotNull(partitions);
    if (mPartitions.size() > 0) {
      mPartitionIndex = 0;
      long blockId = mPartitions.get(0).getBlockId();
      KeyValuePartitionReader reader = KeyValuePartitionReader.Factory.create(blockId);
      mPartitionIterator = reader.iterator();
    }
  }

  @Override
  public boolean hasNext() {
    return mPartitionIterator != null && mPartitionIterator.hasNext();
  }

  @Override
  public KeyValuePair next() throws IOException, AlluxioException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    KeyValuePair ret = mPartitionIterator.next();
    if (!mPartitionIterator.hasNext()) {
      // Current partition has all been iterated, move to next partition.
      mPartitionIndex++;
      if (mPartitionIndex < mPartitions.size()) {
        long blockId = mPartitions.get(mPartitionIndex).getBlockId();
        KeyValuePartitionReader reader = KeyValuePartitionReader.Factory.create(blockId);
        mPartitionIterator = reader.iterator();
      } else {
        mPartitionIterator = null;
      }
    }
    return ret;
  }
}
