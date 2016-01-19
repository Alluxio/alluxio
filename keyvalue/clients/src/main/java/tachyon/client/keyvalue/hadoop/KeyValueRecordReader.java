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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.RecordReader;

import tachyon.client.keyvalue.KeyValueIterator;
import tachyon.client.keyvalue.KeyValuePair;
import tachyon.client.keyvalue.KeyValuePartitionReader;
import tachyon.exception.TachyonException;
import tachyon.util.io.BufferUtils;

/**
 * Implements {@link RecordReader}, each record is a key-value pair stored in a partition of the
 * {@link tachyon.client.keyvalue.KeyValueStores}.
 */
final class KeyValueRecordReader implements RecordReader<BytesWritable, BytesWritable> {
  /** The partition reader for reading the key-value pairs */
  private final KeyValuePartitionReader mReader;
  /** The iterator for iterating through all key-value pairs contained in the partition */
  private final KeyValueIterator mKeyValuePairIterator;
  /** Current position in the partition's byte array */
  private int mPos;
  /** Number of key-value pairs visited by the iterator */
  private int mNumVisitedKeyValuePairs;
  /** Number of key-value pairs */
  private final int mNumKeyValuePairs;

  /**
   * Creates a {@link KeyValueRecordReader} for generating key-value pairs of a partition.
   *
   * @param split the split for a block
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if Tachyon error occurs
   */
  public KeyValueRecordReader(KeyValueInputSplit split) throws IOException, TachyonException {
    mReader = KeyValuePartitionReader.Factory.create(split.getPartitionId());
    mKeyValuePairIterator = mReader.iterator();
    mPos = 0;
    mNumVisitedKeyValuePairs = 0;
    mNumKeyValuePairs = mReader.size();
  }

  @Override
  public boolean next(BytesWritable keyWritable, BytesWritable valueWritable) throws IOException {
    if (!mKeyValuePairIterator.hasNext()) {
      return false;
    }

    KeyValuePair pair;
    try {
      pair = mKeyValuePairIterator.next();
    } catch (TachyonException te) {
      throw new IOException(te);
    }

    DataInputStream key = new DataInputStream(new ByteArrayInputStream(
        BufferUtils.newByteArrayFromByteBuffer(pair.getKey())));
    keyWritable.readFields(key);
    key.close();

    DataInputStream value = new DataInputStream(new ByteArrayInputStream(
        BufferUtils.newByteArrayFromByteBuffer(pair.getValue())));
    valueWritable.readFields(value);
    value.close();

    mPos += keyWritable.getLength() + valueWritable.getLength();
    mNumVisitedKeyValuePairs ++;
    return true;
  }

  @Override
  public BytesWritable createKey() {
    return new BytesWritable(new byte[0]);
  }

  @Override
  public BytesWritable createValue() {
    return new BytesWritable(new byte[0]);
  }

  @Override
  public long getPos() throws IOException {
    return mPos;
  }

  @Override
  public void close() throws IOException {
    mReader.close();
  }

  @Override
  public float getProgress() throws IOException {
    if (mNumKeyValuePairs == 0) {
      return 1.0f;
    }
    return ((float) mNumVisitedKeyValuePairs) / mNumKeyValuePairs;
  }
}
