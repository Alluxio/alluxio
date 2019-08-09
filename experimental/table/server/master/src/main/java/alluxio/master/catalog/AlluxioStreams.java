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

package alluxio.master.catalog;

import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.DelegatingOutputStream;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  static SeekableInputStream wrap(FileInStream stream) {
    return new AlluxioSeekableInputStream(stream);
  }

  /**
   * Wraps a {@link FileOutStream} in a {@link PositionOutputStream} implementation for
   * writers.
   *
   * @param stream a Alluxio FileOutStream
   * @return a PositionOutputStream
   */
  static PositionOutputStream wrap(FileOutStream stream) {
    return new AlluxioPositionOutputStream(stream);
  }

  /**
   * SeekableInputStream implementation for FSDataInputStream that implements ByteBufferReadable in
   * Alluxio
   */
  private static class AlluxioSeekableInputStream extends SeekableInputStream
      implements DelegatingInputStream {
    private final FileInStream stream;
    private final StackTraceElement[] createStack;
    private boolean closed;

    AlluxioSeekableInputStream(FileInStream stream) {
      this.stream = stream;
      this.createStack = Thread.currentThread().getStackTrace();
      this.closed = false;
    }

    @Override
    public InputStream getDelegate() {
      return stream;
    }

    @Override
    public void close() throws IOException {
      stream.close();
      this.closed = true;
    }

    @Override
    public long getPos() throws IOException {
      return stream.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      stream.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      return stream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return stream.read(b, off, len);
    }

    @SuppressWarnings("checkstyle:NoFinalizer")
    @Override
    protected void finalize() throws Throwable {
      super.finalize();
      if (!closed) {
        close(); // releasing resources is more important than printing the warning
        String trace = Joiner.on("\n\t").join(
            Arrays.copyOfRange(createStack, 1, createStack.length));
        LOG.warn("Unclosed input mStream created by:\n\t{}", trace);
      }
    }
  }

  /**
   * PositionOutputStream implementation for FSDataOutputStream.
   */
  private static class AlluxioPositionOutputStream extends PositionOutputStream implements DelegatingOutputStream {
    private final OutputStream mStream;
    private final StackTraceElement[] mCreateStack;
    private boolean mClosed;
    private long mPosition;

    AlluxioPositionOutputStream(FileOutStream stream){
      mStream = stream;
      mCreateStack = Thread.currentThread().getStackTrace();
      mClosed = false;
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
      this.mClosed = true;
    }

    @SuppressWarnings("checkstyle:NoFinalizer")
    @Override
    protected void finalize() throws Throwable {
      super.finalize();
      if (!mClosed) {
        close(); // releasing resources is more important than printing the warning
        String trace = Joiner.on("\n\t").join(
            Arrays.copyOfRange(mCreateStack, 1, mCreateStack.length));
        LOG.warn("Unclosed output mStream created by:\n\t{}", trace);
      }
    }
  }
}
