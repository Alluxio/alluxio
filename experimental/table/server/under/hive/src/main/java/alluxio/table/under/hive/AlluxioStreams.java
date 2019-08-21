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

package alluxio.table.under.hive;

import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.underfs.SeekableUnderFileInputStream;

import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.DelegatingOutputStream;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Convenience methods to get Parquet abstractions for Alluxio data streams.
 *
 * This class is based on Hadoop's HadoopStreams.
 */
class AlluxioStreams {

  private AlluxioStreams() {}

  private static final Logger LOG = LoggerFactory.getLogger(AlluxioStreams.class);

  /**
   * Wraps a {@link FileInStream} in a {@link SeekableInputStream} implementation for readers.
   *
   * @param stream an Alluxio FileInStream
   * @return a SeekableInputStream
   */
  static SeekableInputStream wrap(SeekableUnderFileInputStream stream) {
    return new AlluxioSeekableUFSInputStream(stream);
  }

  /**
   * Wraps a {@link FileOutStream} in a {@link PositionOutputStream} implementation for
   * writers.
   *
   * @param stream a Alluxio FileOutStream
   * @return a PositionOutputStream
   */
  static PositionOutputStream wrap(OutputStream stream) {
    return new AlluxioPositionOutputStream(stream);
  }

  public static SeekableInputStream wrap(InputStream input) {
    return new AlluxioSeekableInputStream(input);
  }

  private static class AlluxioSeekableInputStream extends SeekableInputStream
      implements DelegatingInputStream {
    private final InputStream mStream;
    private long mPos;

    AlluxioSeekableInputStream(InputStream stream) {
      mPos = 0;
      mStream = stream;
    }

    @Override
    public InputStream getDelegate() {
      return mStream;
    }

    @Override
    public void close() throws IOException {
      mStream.close();
    }

    @Override
    public long getPos() throws IOException {
      return mPos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (mPos >= newPos) {
        mStream.reset();
        mStream.skip(newPos);
      } else {
        mStream.skip(newPos - mPos);
      }
      mPos = newPos;
    }

    @Override
    public int read() throws IOException {
      mPos++;
      return mStream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int read = mStream.read(b, off, len);
      mPos += read;
      return read;
    }
  }

  /**
   * SeekableInputStream implementation for SeekableInputStream in Alluxio.
   */
  private static class AlluxioSeekableUFSInputStream extends SeekableInputStream
      implements DelegatingInputStream {
    private final SeekableUnderFileInputStream mStream;

    AlluxioSeekableUFSInputStream(SeekableUnderFileInputStream stream) {
      mStream = stream;
    }

    @Override
    public InputStream getDelegate() {
      return mStream;
    }

    @Override
    public void close() throws IOException {
      mStream.close();
    }

    @Override
    public long getPos() throws IOException {
      return mStream.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      mStream.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      return mStream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return mStream.read(b, off, len);
    }
  }

  /**
   * PositionOutputStream implementation for FSDataOutputStream.
   */
  private static class AlluxioPositionOutputStream extends PositionOutputStream
      implements DelegatingOutputStream {
    private final OutputStream mStream;
    private long mPosition;

    AlluxioPositionOutputStream(OutputStream stream) {
      mStream = stream;
      mPosition = 0;
    }

    @Override
    public OutputStream getDelegate() {
      return mStream;
    }

    @Override
    public long getPos() throws IOException {
      return mPosition;
    }

    @Override
    public void write(int b) throws IOException {
      mStream.write(b);
      mPosition++;
    }

    @Override
    public void write(byte[] b) throws IOException {
      mStream.write(b);
      mPosition += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      mStream.write(b, off, len);
      mPosition += len;
    }

    @Override
    public void flush() throws IOException {
      mStream.flush();
    }

    @Override
    public void close() throws IOException {
      mStream.close();
    }
  }
}
