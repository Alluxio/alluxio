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
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.http.annotation.ThreadSafe;

import alluxio.AlluxioURI;
import alluxio.client.keyvalue.KeyValueStoreWriter;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.AlluxioException;

/**
 * A {@link RecordWriter} to write key-value pairs into a temporary key-value store.
 */
@ThreadSafe
class KeyValueRecordWriter implements RecordWriter<BytesWritable, BytesWritable> {
  private final KeyValueStoreWriter mWriter;
  private final Progressable mProgress;

  /**
   * Constructs a new {@link KeyValueRecordWriter}.
   *
   * @param storeUri the URI for the temporary key-value store to be created by this record writer
   * @param progress the object to be used for reporting progress
   * @throws IOException when instance creation fails
   */
  public KeyValueRecordWriter(AlluxioURI storeUri, Progressable progress) throws IOException {
    try {
      mWriter = KeyValueSystem.Factory.create().createStore(storeUri);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    mProgress = progress;
  }

  @Override
  public synchronized void write(BytesWritable key, BytesWritable value) throws IOException {
    try {
      // NOTE: BytesWritable.getBytes() returns the internal byte array, whose length might not be
      // the same as BytesWritable.getLength().
      mWriter.put(Arrays.copyOf(key.getBytes(), key.getLength()),
          Arrays.copyOf(value.getBytes(), value.getLength()));
      // Sends a progress to the job manager to inform it that the task is still running.
      mProgress.progress();
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void close(Reporter reporter) throws IOException {
    // Completes the new store.
    mWriter.close();
  }
}
