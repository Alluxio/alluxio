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

package alluxio.client.keyvalue;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import alluxio.Constants;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.ByteIOUtils;

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
