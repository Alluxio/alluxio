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

package tachyon.client.block;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Test class for mocking BufferedBlockOutStream and exposing internal state.
 */
public class TestBufferedBlockOutStream extends BufferedBlockOutStream {
  // Shouldn't need more than this for unit tests
  private static final int MAX_DATA = 1000;
  private ByteBuffer mDataWritten;
  private boolean mCanceled;

  public byte[] mLastBufferedWriteArray;
  public int mLastBufferedWriteOffset;
  public int mLastBufferedWriteLen;
  public boolean mHasFlushed;

  public TestBufferedBlockOutStream(long blockId, long blockSize) {
    super(blockId, blockSize);
    mDataWritten = ByteBuffer.allocate(MAX_DATA);
    mCanceled = false;
  }

  /**
   * @return all data which has even been written through the stream via write()
   */
  public byte[] getDataWritten() {
    flush();
    return Arrays.copyOfRange(mDataWritten.array(), 0, (int) mWrittenBytes);
  }

  public void setWrittenBytes(long numBytes) {
    mWrittenBytes = numBytes;
  }

  public int getBytesWritten() {
    return (int) mWrittenBytes;
  }

  public ByteBuffer getBuffer() {
    return mBuffer;
  }

  public boolean isCanceled() {
    return mCanceled;
  }

  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public void cancel() throws IOException {
    mCanceled = true;
    close();
  }

  @Override
  public void close() {
    mClosed = true;
  }

  @Override
  public void flush() {
    int bytesToWrite = mBuffer.position();
    mDataWritten.put(mBuffer.array(), 0, bytesToWrite);
    mFlushedBytes += bytesToWrite;
    mBuffer.clear();
    mHasFlushed = true;
  }

  /**
   * These writes are generally really large, so instead of doing the write, just record the
   * parameters so that they can be verified by tests.
   */
  @Override
  protected void unBufferedWrite(byte[] b, int off, int len) throws IOException {
    mLastBufferedWriteArray = b;
    mLastBufferedWriteOffset = off;
    mLastBufferedWriteLen = len;
  }
}

