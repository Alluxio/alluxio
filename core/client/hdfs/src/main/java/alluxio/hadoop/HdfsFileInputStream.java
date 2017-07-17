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

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;

import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An input stream for reading a file from HDFS. This is just a wrapper around
 * {@link FileInStream} with additional statistics gathering in a {@link Statistics} object.
 */
@NotThreadSafe
public class HdfsFileInputStream extends InputStream implements Seekable, PositionedReadable {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsFileInputStream.class);

  private final Statistics mStatistics;
  private final FileInStream mInputStream;

  private boolean mClosed = false;

  /**
   * Constructs a new stream for reading a file from HDFS.
   *
   * @param context the file system context
   * @param uri the Alluxio file URI
   * @param stats filesystem statistics
   */
  public HdfsFileInputStream(FileSystemContext context, AlluxioURI uri, Statistics stats)
      throws IOException {
    LOG.debug("HdfsFileInputStream({}, {})", uri, stats);

    mStatistics = stats;
    FileSystem fs = FileSystem.Factory.get(context);
    try {
      mInputStream = fs.openFile(uri, OpenFileOptions.defaults());
    } catch (FileDoesNotExistException e) {
      // Transform the Alluxio exception to a Java exception to satisfy the HDFS API contract.
      throw new FileNotFoundException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(uri));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int available() throws IOException {
    if (mClosed) {
      throw new IOException("Cannot query available bytes from a closed stream.");
    }
    return (int) mInputStream.remaining();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mInputStream.close();
    mClosed = true;
  }

  @Override
  public long getPos() throws IOException {
    return mInputStream.getPos();
  }

  @Override
  public int read() throws IOException {
    if (mClosed) {
      throw new IOException(ExceptionMessage.READ_CLOSED_STREAM.getMessage());
    }

    int ret = mInputStream.read();
    if (ret != -1 && mStatistics != null) {
      mStatistics.incrementBytesRead(1);
    }
    return ret;
  }

  @Override
  public int read(byte[] buffer) throws IOException {
    return read(buffer, 0, buffer.length);
  }

  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    if (mClosed) {
      throw new IOException(ExceptionMessage.READ_CLOSED_STREAM.getMessage());
    }

    int ret = mInputStream.read(buffer, offset, length);
    if (ret != -1 && mStatistics != null) {
      mStatistics.incrementBytesRead(ret);
    }
    return ret;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    if (mClosed) {
      throw new IOException(ExceptionMessage.READ_CLOSED_STREAM.getMessage());
    }

    int ret = mInputStream.positionedRead(position, buffer, offset, length);
    if (ret != -1 && mStatistics != null) {
      mStatistics.incrementBytesRead(ret);
    }
    return ret;
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    int n = 0; // total bytes read
    while (n < length) {
      int ret = read(position + n, buffer, offset + n, length - n);
      if (ret == -1) {
        throw new EOFException();
      }
      n += ret;
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    mInputStream.seek(pos);
  }

  /**
   * This method is not supported in {@link HdfsFileInputStream}.
   *
   * @param targetPos N/A
   * @return N/A
   * @throws IOException always
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException(ExceptionMessage.NOT_SUPPORTED.getMessage());
  }

  @Override
  public long skip(long n) throws IOException {
    if (mClosed) {
      throw new IOException("Cannot skip bytes in a closed stream.");
    }
    return mInputStream.skip(n);
  }
}
