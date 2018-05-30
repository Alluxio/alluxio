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

package alluxio.network.thrift;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.google.common.base.Preconditions;

/**
 * A transport that one can peek a given number of bytes from the read stream without changing the
 * read position.
 */
public class PeekableTransport extends TTransport {
  private TTransport mBaseTransport;
  private byte[] mPeekBuffer;
  private int mPos;
  private int mBufferSize;

  /**
   * @param baseTransport the base transport to peek
   */
  public PeekableTransport(TTransport baseTransport) {
    mBaseTransport = baseTransport;
  }

  @Override
  public boolean isOpen() {
    return mBaseTransport.isOpen();
  }

  @Override
  public void open() throws TTransportException {
    mBaseTransport.open();
  }

  @Override
  public void close() {
    resetBuffer();
    mBaseTransport.close();
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    int readFromBuffer = Math.min(getBytesRemainingInBuffer(), len);
    if (readFromBuffer > 0) {
      System.arraycopy(mPeekBuffer, mPos, buf, off, readFromBuffer);
      consumeBuffer(readFromBuffer);
      if (getBytesRemainingInBuffer() == 0) {
        resetBuffer();
      }
    }
    int readFromTransport =
        mBaseTransport.read(buf, off + readFromBuffer, len - readFromBuffer);
    return readFromBuffer + readFromTransport;
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    mBaseTransport.write(buf, off, len);
  }

  @Override
  public void flush() throws TTransportException {
    mBaseTransport.flush();
  }

  /**
   * Peaks up to len bytes into buffer buf, starting at offset off. This method will not change the
   * underlying position of the read stream.
   *
   * @param buf Array to read into
   * @param off Index to start reading at
   * @param len Maximum number of bytes to read
   * @return The number of bytes actually read
   * @throws TTransportException if there was an error reading data
   */
  public int peek(byte[] buf, int off, int len) throws TTransportException {
    Preconditions.checkState(mPeekBuffer == null, "Currently we only support peek once");
    int bytesRead = mBaseTransport.read(buf, off, len);
    if (bytesRead > 0) {
      mPeekBuffer = new byte[bytesRead];
      mBufferSize = bytesRead;
      mPos = 0;
      System.arraycopy(buf, off, mPeekBuffer, mPos, mBufferSize);
    }
    return bytesRead;
  }

  /**
   * @return current buffer position
   */
  public int getBufferPosition() {
    return mPos;
  }

  /**
   * @return the number of bytes left in buffer
   */
  public int getBytesRemainingInBuffer() {
    return mBufferSize - mPos;
  }

  /**
   * Consumes len bytes from the buffer.
   *
   * @param len number of bytes to consume
   */
  public void consumeBuffer(int len) {
    mPos += len;
  }

  /**
   * Resets the buffer to null and updates the position states.
   */
  private void resetBuffer() {
    mBufferSize = 0;
    mPos = 0;
    mPeekBuffer = null;
  }
}
