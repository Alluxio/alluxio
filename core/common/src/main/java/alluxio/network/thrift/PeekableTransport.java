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
  protected TTransport mUnderlyingTransport;
  private byte[] mBuffer;
  private int mPos;
  private int mBufferSize;

  /**
   * @param baseTransport the base transport to peek
   */
  public PeekableTransport(TTransport baseTransport) {
    mUnderlyingTransport = baseTransport;
  }

  @Override
  public boolean isOpen() {
    return mUnderlyingTransport.isOpen();
  }

  @Override
  public void open() throws TTransportException {
    mUnderlyingTransport.open();
  }

  @Override
  public void close() {
    mUnderlyingTransport.close();
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    int bytesRemaining = getBytesRemainingInBuffer();
    int readFromBuffer = (len > bytesRemaining ? bytesRemaining : len);
    if (readFromBuffer > 0) {
      System.arraycopy(mBuffer, mPos, buf, off, readFromBuffer);
      consumeBuffer(readFromBuffer);
    }
    int readFromTransport =
        mUnderlyingTransport.read(buf, off + readFromBuffer, len - readFromBuffer);
    return readFromBuffer + readFromTransport;
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    mUnderlyingTransport.write(buf, off, len);
  }

  @Override
  public void flush() throws TTransportException {
    mUnderlyingTransport.flush();
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
    Preconditions.checkState(mBuffer == null, "Currently we only support peek once");
    int bytesRead = mUnderlyingTransport.read(buf, off, len);
    if (bytesRead > 0) {
      mBuffer = new byte[bytesRead];
      mBufferSize = bytesRead;
      mPos = 0;
      System.arraycopy(buf, off, mBuffer, mPos, mBufferSize);
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
}
