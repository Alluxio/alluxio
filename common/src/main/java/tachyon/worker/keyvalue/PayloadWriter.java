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

package tachyon.worker.keyvalue;

import tachyon.util.io.ByteIOUtils;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Writer to create key-value payload.
 */
public final class PayloadWriter implements Closeable, Flushable {
  private OutputStream mOutputStream;

  public PayloadWriter(OutputStream out) {
    mOutputStream = out;
  }

  @Override
  public void flush() throws IOException {
    mOutputStream.flush();
  }

  @Override
  public void close() throws IOException {
    mOutputStream.close();
  }

  /**
   * Puts a key into payload.
   *
   * @param key bytes of key
   * @param value offset of this key in payload
   * @throws IOException if error occurs writing the key and value to output stream
   */
  public void addKeyAndValue(byte[] key, byte[] value) throws IOException {
    // Pack key and value into a byte array
    ByteIOUtils.writeInt(mOutputStream, key.length);
    ByteIOUtils.writeInt(mOutputStream, value.length);
    mOutputStream.write(key);
    mOutputStream.write(value);
  }
}
