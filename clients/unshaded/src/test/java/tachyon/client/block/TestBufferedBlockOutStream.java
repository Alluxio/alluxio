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

public class TestBufferedBlockOutStream extends BufferedBlockOutStream {

  // Shouldn't need more than this for unit tests
  private static final int MAX_DATA = 1000;
  private ByteBuffer mDataWritten;
  private boolean mCancelled;

  public byte[] mLastBufferedWriteArray;
  public int mLastBufferedWriteOffset;
  public int mLastBufferedWriteLen;
  public boolean mHasFlushed;

  public TestBufferedBlockOutStream(long blockId, long blockSize) {
    super(blockId, blockSize);
    mDataWritten = ByteBuffer.allocate(MAX_DATA);
    mCancelled = false;
  }

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

  public boolean isCancelled() {
    return mCancelled;
  }

  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public void cancel() throws IOException {
    mCancelled = true;
    mClosed = true;
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

  @Override
  protected void unBufferedWrite(byte[] b, int off, int len) throws IOException {
    mLastBufferedWriteArray = b;
    mLastBufferedWriteOffset = off;
    mLastBufferedWriteLen = len;
  }
}

