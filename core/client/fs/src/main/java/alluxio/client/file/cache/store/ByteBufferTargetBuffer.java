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

package alluxio.client.file.cache.store;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Target buffer backed by nio ByteBuffer for zero-copy read from page store.
 */
public class ByteBufferTargetBuffer implements PageReadTargetBuffer {
  private final ByteBuffer mTarget;

  /**
   * Constructor.
   * @param target
   */
  public ByteBufferTargetBuffer(ByteBuffer target) {
    mTarget = target;
  }

  @Override
  public byte[] byteArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer byteBuffer() {
    return mTarget;
  }

  @Override
  public long offset() {
    return mTarget.position();
  }

  @Override
  public WritableByteChannel byteChannel() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long remaining() {
    return mTarget.remaining();
  }

  @Override
  public void writeBytes(byte[] srcArray, int srcOffset, int length) {
    mTarget.put(srcArray, srcOffset, length);
  }

  @Override
  public int readFromFile(RandomAccessFile file, int length) throws IOException {
    return file.getChannel().read(mTarget);
  }
}
