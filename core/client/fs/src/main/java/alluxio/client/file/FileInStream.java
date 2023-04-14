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

package alluxio.client.file;

import alluxio.Seekable;
import alluxio.client.BoundedStream;
import alluxio.client.PositionedReadable;
import alluxio.exception.PreconditionMessage;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * A streaming API to read a file. This API represents a file as a stream of bytes and provides a
 * collection of {@link #read} methods to access this stream of bytes. In addition, one can seek
 * into a given offset of the stream to read.
 */
public abstract class FileInStream extends InputStream
    implements BoundedStream, PositionedReadable, Seekable {
  private final byte[] mSingleByte = new byte[1];

  @Override
  public int read() throws IOException {
    int bytesRead = read(mSingleByte);
    if (bytesRead == -1) {
      return -1;
    }
    Preconditions.checkState(bytesRead == 1);
    return BufferUtils.byteToInt(mSingleByte[0]);
  }

  @Override
  public int read(byte[] b) throws IOException {
    Preconditions.checkNotNull(b, PreconditionMessage.ERR_READ_BUFFER_NULL);
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkNotNull(b, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(len >= 0 && off >= 0 && len <= b.length - off,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    return read(ByteBuffer.wrap(b, off, len));
  }

  /**
   * Reads up to buf.remaining() bytes into buf. Callers should use
   * buf.limit(..) to control the size of the desired read.
   * <p>
   * After a successful call, buf.position() will be advanced by the number
   * of bytes read and buf.limit() should be unchanged.
   * <p>
   * In the case of an exception, the values of buf.position() and buf.limit()
   * are undefined, and callers should be prepared to recover from this eventually.
   * <p>
   * Implementations should treat 0-length requests as legitimate, and must not
   * signal an error upon their receipt.
   *
   * @param byteBuffer the ByteBuffer to receive the results of the read operation
   * @return the number of bytes read, possibly zero, or -1 if reach end-of-stream
   */
  public int read(ByteBuffer byteBuffer) throws IOException {
    final int bytesToRead = byteBuffer.remaining();
    if (byteBuffer.hasArray()) {
      byte[] array = byteBuffer.array();
      int arrayOffset = byteBuffer.arrayOffset();
      int bytesRead = read(array, arrayOffset, bytesToRead);
      if (bytesRead > 0) {
        byteBuffer.position(byteBuffer.position() + bytesRead);
      }
      return bytesRead;
    }

    byte[] array = new byte[bytesToRead];
    int bytesRead = read(array, 0, bytesToRead);
    if (bytesRead == -1) {
      return -1;
    }
    byteBuffer.put(array, 0, bytesRead);
    return bytesRead;
  }
}
