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

package alluxio.client.keyvalue;

import alluxio.client.AbstractOutStream;
import alluxio.util.io.ByteIOUtils;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An implementation of {@link PayloadWriter} using stream API to write to underlying payload
 * storage.
 *
 * For each key-value pair, this writer appends the following 4 pieces of data in order: (1)
 * keyLength (4 bytes) (2) valueLength (4 bytes) (3) keyData (keyLength bytes) (4) valueData
 * (valueLength bytes).
 */
@NotThreadSafe
final class BasePayloadWriter implements Closeable, Flushable, PayloadWriter {
  private AbstractOutStream mOutStream;

  /**
   * Constructs a {@link BasePayloadWriter} instance.
   *
   * @param out the stream to output payload
   */
  BasePayloadWriter(AbstractOutStream out) {
    mOutStream = Preconditions.checkNotNull(out);
  }

  @Override
  public void flush() throws IOException {
    mOutStream.flush();
  }

  @Override
  public void close() throws IOException {
    mOutStream.close();
  }

  @Override
  public int insert(byte[] key, byte[] value) throws IOException {
    int offset = mOutStream.getBytesWritten();
    // Pack key and value into a byte array
    ByteIOUtils.writeInt(mOutStream, key.length);
    ByteIOUtils.writeInt(mOutStream, value.length);
    mOutStream.write(key);
    mOutStream.write(value);
    return offset;
  }
}
