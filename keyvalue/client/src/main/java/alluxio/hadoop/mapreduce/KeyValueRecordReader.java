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

import alluxio.client.keyvalue.KeyValueIterator;
import alluxio.client.keyvalue.KeyValuePair;
import alluxio.client.keyvalue.KeyValuePartitionReader;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.AlluxioException;
import alluxio.util.io.BufferUtils;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implements {@link RecordReader} that breaks the input from a key-value store data into records
 * and input records to the Mapper. Each record is a key-value pair stored in a partition of the
 * {@link KeyValueSystem}.
 */
@ThreadSafe
final class KeyValueRecordReader extends RecordReader<BytesWritable, BytesWritable> {
  /** The partition reader for reading the key-value pairs. */
  private KeyValuePartitionReader mReader;
  /** The iterator for iterating through all key-value pairs contained in the partition. */
  private KeyValueIterator mKeyValuePairIterator;
  /** Number of key-value pairs visited by the iterator. */
  private int mNumVisitedKeyValuePairs;
  /** Number of key-value pairs. */
  private int mNumKeyValuePairs;
  /** Current key. */
  private BytesWritable mCurrentKey;
  /** Current value. */
  private BytesWritable mCurrentValue;

  /**
   * Creates a {@link KeyValueRecordReader} for generating key-value pairs of a partition.
   */
  public KeyValueRecordReader() {
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    try {
      mReader =
          KeyValuePartitionReader.Factory.create(((KeyValueInputSplit) split).getPartitionId());
      mKeyValuePairIterator = mReader.iterator();
      mNumVisitedKeyValuePairs = 0;
      mNumKeyValuePairs = mReader.size();
      mCurrentKey = new BytesWritable();
      mCurrentValue = new BytesWritable();
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized BytesWritable getCurrentKey() {
    return mCurrentKey;
  }

  @Override
  public synchronized BytesWritable getCurrentValue() {
    return mCurrentValue;
  }

  @Override
  public synchronized boolean nextKeyValue() throws IOException {
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
    mCurrentKey.set(new BytesWritable(BufferUtils.newByteArrayFromByteBuffer(pair.getKey())));
    mCurrentValue.set(new BytesWritable(BufferUtils.newByteArrayFromByteBuffer(pair.getValue())));
    mNumVisitedKeyValuePairs++;
    return true;
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
