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

package tachyon.client.keyvalue;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import tachyon.exception.TachyonException;
import tachyon.thrift.PartitionInfo;

/**
 * {@link KeyValueIterator} to iterate over key-value pairs in {@link KeyValueStores}.
 */
@NotThreadSafe
public final class KeyValueStoreIterator implements KeyValueIterator {
  /** All partitions */
  private List<PartitionInfo> mPartitions;
  /** Index of the partition being visited */
  private int mPartitionIndex = -1;
  /** Iterator of the partition being visited */
  private KeyValueIterator mPartitionIterator;

  public KeyValueStoreIterator(List<PartitionInfo> partitions)
      throws IOException, TachyonException {
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
  public KeyValuePair next() throws IOException, TachyonException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    KeyValuePair ret = mPartitionIterator.next();
    if (!mPartitionIterator.hasNext()) {
      // Current partition has all been iterated, move to next partition.
      mPartitionIndex += 1;
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

