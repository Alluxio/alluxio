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

package tachyon.network.protocol.databuffer;

import java.nio.ByteBuffer;

import io.netty.buffer.Unpooled;

/**
 * A DataBuffer backed by a byte[].
 */
public class DataByteArrayChannel extends DataBuffer {
  private final byte[] mByteArray;
  private final long mOffset;
  private final long mLength;

  /**
   *
   * @param byteArray The array representing the data.
   * @param offset The offset into the byteArray.
   * @param length The length of the data.
   */
  public DataByteArrayChannel(byte[] byteArray, long offset, long length) {
    mByteArray = byteArray;
    mOffset = offset;
    mLength = length;
  }

  @Override
  public Object getNettyOutput() {
    return Unpooled.wrappedBuffer(mByteArray, (int) mOffset, (int) mLength);
  }

  @Override
  public long getLength() {
    return mLength;
  }

  @Override
  public ByteBuffer getReadOnlyByteBuffer() {
    return ByteBuffer.wrap(mByteArray, (int) mOffset, (int) mLength).asReadOnlyBuffer();
  }
}
