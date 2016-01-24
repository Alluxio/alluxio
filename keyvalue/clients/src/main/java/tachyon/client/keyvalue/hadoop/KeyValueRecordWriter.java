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

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.http.annotation.ThreadSafe;

import tachyon.TachyonURI;
import tachyon.client.keyvalue.KeyValueStoreWriter;
import tachyon.client.keyvalue.KeyValueStores;
import tachyon.exception.TachyonException;

/**
 * It writes key-value pairs into a temporary key-value store.
 */
@ThreadSafe
class KeyValueRecordWriter implements RecordWriter<BytesWritable, BytesWritable> {
  private static final KeyValueStores KEY_VALUE_STORES = KeyValueStores.Factory.create();

  private final KeyValueStoreWriter mWriter;
  private final Progressable mProgress;

  /**
   * Constructs a new {@link KeyValueRecordWriter}.
   *
   * @param storeUri the URI for the temporary key-value store to be created by this record writer
   * @param progress the object to be used for reporting progress
   * @throws IOException when instance creation fails
   */
  public KeyValueRecordWriter(TachyonURI storeUri, Progressable progress) throws IOException {
    try {
      mWriter = KEY_VALUE_STORES.create(storeUri);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
    mProgress = progress;
  }

  /**
   * Copies a byte array of the specified length from the specified array, starting at offset 0.
   *
   * @param src the source array
   * @param length the length of bytes to be copied
   * @return the copied array
   */
  private byte[] copyBytes(byte[] src, int length) {
    byte[] result = new byte[length];
    System.arraycopy(src, 0, result, 0, length);
    return result;
  }

  @Override
  public synchronized void write(BytesWritable key, BytesWritable value) throws IOException {
    try {
      mWriter.put(copyBytes(key.getBytes(), key.getLength()), copyBytes(value.getBytes(),
          value.getLength()));
      // Sends a progress to the job manager to inform it that the task is still running.
      mProgress.progress();
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void close(Reporter reporter) throws IOException {
    // Completes the new store.
    mWriter.close();
  }
}
