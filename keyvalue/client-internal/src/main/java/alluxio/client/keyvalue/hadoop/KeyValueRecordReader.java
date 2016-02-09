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

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.RecordReader;

import alluxio.client.keyvalue.KeyValueIterator;
import alluxio.client.keyvalue.KeyValuePair;
import alluxio.client.keyvalue.KeyValuePartitionReader;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.AlluxioException;
import alluxio.util.io.BufferUtils;

/**
 * Implements {@link RecordReader}, each record is a key-value pair stored in a partition of the
 * {@link KeyValueSystem}.
 */
@ThreadSafe
final class KeyValueRecordReader implements RecordReader<BytesWritable, BytesWritable> {
  /** The partition reader for reading the key-value pairs. */
  private final KeyValuePartitionReader mReader;
  /** The iterator for iterating through all key-value pairs contained in the partition. */
  private final KeyValueIterator mKeyValuePairIterator;
  /** Accumulated bytes of key-value pairs read so far. */
  private int mKeyValuePairsBytesRead;
  /** Number of key-value pairs visited by the iterator. */
  private int mNumVisitedKeyValuePairs;
  /** Number of key-value pairs. */
  private final int mNumKeyValuePairs;

  /**
   * Creates a {@link KeyValueRecordReader} for generating key-value pairs of a partition.
   *
   * @param split the split for a block
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  public KeyValueRecordReader(KeyValueInputSplit split) throws IOException, AlluxioException {
    mReader = KeyValuePartitionReader.Factory.create(split.getPartitionId());
    mKeyValuePairIterator = mReader.iterator();
    mKeyValuePairsBytesRead = 0;
    mNumVisitedKeyValuePairs = 0;
    mNumKeyValuePairs = mReader.size();
  }

  @Override
  public synchronized boolean next(BytesWritable keyWritable, BytesWritable valueWritable)
      throws IOException {
    if (!mKeyValuePairIterator.hasNext()) {
      return false;
    }

    KeyValuePair pair;
    try {
      pair = mKeyValuePairIterator.next();
    } catch (AlluxioException e) {
      throw new IOException(e);
    }

    // TODO(cc): Implement a ByteBufferInputStream which is backed by a ByteBuffer so we could
    // benefit from zero-copy.
    keyWritable.set(new BytesWritable(BufferUtils.newByteArrayFromByteBuffer(pair.getKey())));
    valueWritable.set(new BytesWritable(BufferUtils.newByteArrayFromByteBuffer(pair.getValue())));

    mKeyValuePairsBytesRead += keyWritable.getLength() + valueWritable.getLength();
    mNumVisitedKeyValuePairs++;
    return true;
  }

  @Override
  public BytesWritable createKey() {
    return new BytesWritable();
  }

  @Override
  public BytesWritable createValue() {
    return new BytesWritable();
  }

  /**
   * {@inheritDoc}.
   * <p>
   * @return total bytes of key-value pairs read so far, as an approximation for all read bytes
   */
  @Override
  public synchronized long getPos() throws IOException {
    return mKeyValuePairsBytesRead;
  }

  @Override
  public synchronized void close() throws IOException {
    mReader.close();
  }

  @Override
  public synchronized float getProgress() throws IOException {
    if (mNumKeyValuePairs == 0) {
      return 1.0f;
    }
    return ((float) mNumVisitedKeyValuePairs) / mNumKeyValuePairs;
  }
}
