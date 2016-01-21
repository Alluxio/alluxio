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
import org.apache.http.annotation.ThreadSafe;

import tachyon.client.keyvalue.KeyValueStoreWriter;
import tachyon.exception.TachyonException;

/**
 * A {@link RecordWriter} to write key-value pairs output by Reducers to a
 * {@link tachyon.client.keyvalue.KeyValueStore}.
 */
@ThreadSafe
class KeyValueRecordWriter implements RecordWriter<BytesWritable, BytesWritable> {
  private KeyValueStoreWriter mWriter;

  public KeyValueRecordWriter(KeyValueStoreWriter writer) {
    mWriter = writer;
  }

  @Override
  public synchronized void write(BytesWritable key, BytesWritable value) throws IOException {
    try {
      mWriter.put(key.getBytes(), value.getBytes());
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void close(Reporter reporter) throws IOException {
    mWriter.close();
  }
}
