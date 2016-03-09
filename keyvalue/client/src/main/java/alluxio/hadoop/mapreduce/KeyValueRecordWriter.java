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

package alluxio.hadoop.mapreduce;

import alluxio.AlluxioURI;
import alluxio.client.keyvalue.KeyValueStoreWriter;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.AlluxioException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link RecordWriter} to write key-value pairs into a temporary key-value store.
 */
@ThreadSafe
final class KeyValueRecordWriter extends RecordWriter<BytesWritable, BytesWritable> {
  private final KeyValueStoreWriter mWriter;

  /**
   * Constructs a new {@link KeyValueRecordWriter}.
   *
   * @param storeUri the URI for the temporary key-value store to be created by this record writer
   * @throws IOException when instance creation fails
   */
  public KeyValueRecordWriter(AlluxioURI storeUri) throws IOException {
    try {
      mWriter = KeyValueSystem.Factory.create().createStore(storeUri);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void write(BytesWritable key, BytesWritable value) throws IOException {
    try {
      // NOTE: BytesWritable.getBytes() returns the internal byte array, whose length might not be
      // the same as BytesWritable.getLength().
      mWriter.put(Arrays.copyOf(key.getBytes(), key.getLength()),
          Arrays.copyOf(value.getBytes(), value.getLength()));
      // Sends a progress to the job manager to inform it that the task is still running.
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void close(TaskAttemptContext context) throws IOException {
    // Completes the new store.
    mWriter.close();
  }
}
