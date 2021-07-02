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

package alluxio.hadoop;

import alluxio.client.file.FileInStream;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A wrapper class to translate Hadoop FileSystem FSDataInputStream to Alluxio FileSystem
 * FileInStream.
 */
public class AlluxioHdfsInputStream extends FileInStream {
  private final FSDataInputStream mInput;

  /**
   * @param input Hadoop FileSystem FSDataInputStream
   */
  public AlluxioHdfsInputStream(FSDataInputStream input) {
    mInput = Preconditions.checkNotNull(input, "null");
  }

  @Override
  public int read(byte[] bytes) throws IOException {
    return mInput.read(bytes);
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    return mInput.read(bytes, offset, length);
  }

  @Override
  public int read() throws IOException {
    return mInput.read();
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    return mInput.read(buf);
  }

  @Override
  public long skip(long length) throws IOException {
    return mInput.skip(length);
  }

  @Override
  public int available() throws IOException {
    return mInput.available();
  }

  @Override
  public void close() throws IOException {
    mInput.close();
  }

  @Override
  public synchronized void mark(int limit) {
    mInput.mark(limit);
  }

  @Override
  public synchronized void reset() throws IOException {
    mInput.reset();
  }

  @Override
  public boolean markSupported() {
    return mInput.markSupported();
  }

  @Override
  public void seek(long position) throws IOException {
    mInput.seek(position);
  }

  @Override
  public long getPos() throws IOException {
    return mInput.getPos();
  }

  // TODO(binfan): implement this method
  @Override
  public long remaining() {
    throw new UnsupportedOperationException("Remaining is not supported");
  }

  @Override
  public int positionedRead(long position, byte[] buffer, int offset, int length)
      throws IOException {
    return mInput.read(position, buffer, offset, length);
  }
}
