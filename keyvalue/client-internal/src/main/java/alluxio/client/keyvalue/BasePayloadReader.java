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

package alluxio.client.keyvalue;

import alluxio.Constants;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.ByteIOUtils;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An implementation of {@link PayloadReader} capable of randomly accessing the underlying payload
 * storage.
 */
@NotThreadSafe
final class BasePayloadReader implements PayloadReader {
  private static final int KEY_DATA_OFFSET = 2 * Constants.BYTES_IN_INTEGER;
  private ByteBuffer mBuf;

  /**
   * Constructs an instance based on an input buffer.
   *
   * @param buf input buffer
   */
  public BasePayloadReader(ByteBuffer buf) {
    mBuf = Preconditions.checkNotNull(buf).duplicate();
  }

  @Override
  public ByteBuffer getKey(int pos) {
    final int keyLength = ByteIOUtils.readInt(mBuf, pos);
    final int keyFrom = pos + KEY_DATA_OFFSET;
    return BufferUtils.sliceByteBuffer(mBuf, keyFrom, keyLength);
  }

  @Override
  public ByteBuffer getValue(int pos) {
    final int keyLength = ByteIOUtils.readInt(mBuf, pos);
    final int valueLength = ByteIOUtils.readInt(mBuf, pos + Constants.BYTES_IN_INTEGER);
    final int valueFrom = pos + KEY_DATA_OFFSET + keyLength;
    return BufferUtils.sliceByteBuffer(mBuf, valueFrom, valueLength);
  }

}
